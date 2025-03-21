import asyncio
import json
import logging
import os
import re
from typing import List, Optional

import aiohttp
import aiohttp_session
import uvloop
from aiohttp import web
from prometheus_async.aio.web import server_stats  # type: ignore

from gear import (
    AuthClient,
    Database,
    Transaction,
    check_csrf_token,
    create_session,
    maybe_parse_bearer_header,
    monitor_endpoints_middleware,
    setup_aiohttp_session,
    transaction,
)
from gear.cloud_config import get_global_config
from hailtop import httpx
from hailtop.config import get_deploy_config
from hailtop.hail_logging import AccessLogger
from hailtop.tls import internal_server_ssl_context
from hailtop.utils import secret_alnum_string
from web_common import render_template, set_message, setup_aiohttp_jinja2, setup_common_static_routes

from .exceptions import (
    AuthUserError,
    DuplicateLoginID,
    DuplicateUsername,
    EmptyLoginID,
    InvalidType,
    InvalidUsername,
    MultipleExistingUsers,
    MultipleUserTypes,
    PreviouslyDeletedUser,
    UnknownUser,
)
from .flow import get_flow_client

log = logging.getLogger('auth')

uvloop.install()

CLOUD = get_global_config()['cloud']
ORGANIZATION_DOMAIN = os.environ['HAIL_ORGANIZATION_DOMAIN']

deploy_config = get_deploy_config()

routes = web.RouteTableDef()

auth = AuthClient()


async def user_from_login_id(db, login_id):
    users = [x async for x in db.select_and_fetchall("SELECT * FROM users WHERE login_id = %s;", login_id)]
    if len(users) == 1:
        return users[0]
    assert len(users) == 0, users
    return None


async def users_with_username_or_login_id(tx: Transaction, username: str, login_id: Optional[str]) -> List[dict]:
    where_conditions = ['username = %s']
    where_args = [username]

    if login_id is not None:
        where_conditions.append('login_id = %s')
        where_args.append(login_id)

    existing_users = [
        x
        async for x in tx.execute_and_fetchall(
            f"SELECT * FROM users WHERE {' OR '.join(where_conditions)} LOCK IN SHARE MODE;", where_args
        )
    ]

    return existing_users


async def check_valid_new_user(tx: Transaction, username, login_id, is_developer, is_service_account) -> Optional[dict]:
    if not isinstance(username, str):
        raise InvalidType('username', username, 'str')
    if login_id is not None and not isinstance(login_id, str):
        raise InvalidType('login_id', login_id, 'str')
    if not isinstance(is_developer, bool):
        raise InvalidType('is_developer', is_developer, 'bool')
    if not isinstance(is_service_account, bool):
        raise InvalidType('is_service_account', is_service_account, 'bool')
    if is_developer and is_service_account:
        raise MultipleUserTypes(username)
    if not is_service_account and not login_id:
        raise EmptyLoginID(username)
    if not username or not all(c for c in username if c.isalnum()):
        raise InvalidUsername(username)

    existing_users = await users_with_username_or_login_id(tx, username, login_id)

    if len(existing_users) > 1:
        raise MultipleExistingUsers(username, login_id)

    if len(existing_users) == 1:
        existing_user = existing_users[0]
        expected_username = existing_user['username']
        expected_login_id = existing_user['login_id']
        if username != expected_username:
            raise DuplicateLoginID(expected_username, login_id)
        if login_id != expected_login_id:
            raise DuplicateUsername(username, expected_login_id)
        if existing_user['state'] in ('deleting', 'deleted'):
            raise PreviouslyDeletedUser(username)
        return existing_user

    return None


async def insert_new_user(
    db: Database, username: str, login_id: Optional[str], is_developer: bool, is_service_account: bool
) -> bool:
    @transaction(db)
    async def _insert(tx):
        existing_user = await check_valid_new_user(tx, username, login_id, is_developer, is_service_account)
        if existing_user is not None:
            return False

        await tx.execute_insertone(
            '''
INSERT INTO users (state, username, login_id, is_developer, is_service_account)
VALUES (%s, %s, %s, %s, %s);
''',
            ('creating', username, login_id, is_developer, is_service_account),
        )

    await _insert()  # pylint: disable=no-value-for-parameter
    return True


def cleanup_session(session):
    def _delete(key):
        if key in session:
            del session[key]

    _delete('pending')
    _delete('login_id')
    _delete('next')
    _delete('caller')
    _delete('session_id')
    _delete('flow')


@routes.get('/healthcheck')
async def get_healthcheck(request):  # pylint: disable=W0613
    return web.Response()


@routes.get('')
@routes.get('/')
@auth.web_maybe_authenticated_user
async def get_index(request, userdata):  # pylint: disable=unused-argument
    return await render_template('auth', request, userdata, 'index.html', {})


@routes.get('/creating')
@auth.web_maybe_authenticated_user
async def creating_account(request, userdata):
    db = request.app['db']
    session = await aiohttp_session.get_session(request)
    if 'pending' in session:
        login_id = session['login_id']
        user = await user_from_login_id(db, login_id)

        next_url = deploy_config.external_url('auth', '/user')
        next_page = session.pop('next', next_url)

        cleanup_session(session)

        if user is None:
            set_message(session, f'Account does not exist for login id {login_id}.', 'error')
            return aiohttp.web.HTTPFound(deploy_config.external_url('auth', ''))

        page_context = {'username': user['username'], 'state': user['state'], 'login_id': user['login_id']}

        if user['state'] == 'deleting' or user['state'] == 'deleted':
            return await render_template('auth', request, userdata, 'account-error.html', page_context)

        if user['state'] == 'active':
            session_id = await create_session(db, user['id'])
            session['session_id'] = session_id
            set_message(session, f'Account has been created for {user["username"]}.', 'info')
            return aiohttp.web.HTTPFound(next_page)

        assert user['state'] == 'creating'
        session['pending'] = True
        session['login_id'] = login_id
        session['next'] = next_page
        return await render_template('auth', request, userdata, 'account-creating.html', page_context)

    return aiohttp.web.HTTPUnauthorized()


@routes.get('/creating/wait')
async def creating_account_wait(request):
    session = await aiohttp_session.get_session(request)
    if 'pending' not in session:
        raise web.HTTPUnauthorized()
    return await _wait_websocket(request, session['login_id'])


async def _wait_websocket(request, login_id):
    app = request.app
    db = app['db']

    user = await user_from_login_id(db, login_id)
    if not user:
        return web.HTTPNotFound()

    ws = web.WebSocketResponse()
    await ws.prepare(request)

    try:
        count = 0
        while count < 10:
            try:
                user = await user_from_login_id(db, login_id)
                assert user
                if user['state'] != 'creating':
                    log.info(f"user {user['username']} is no longer creating")
                    break
            except asyncio.CancelledError:
                raise
            except Exception:  # pylint: disable=broad-except
                log.exception(f"/creating/wait: error while updating status for user {user['username']}")
            await asyncio.sleep(1)
            count += 1

        if count >= 10:
            log.info(f"user {user['username']} is still in state creating")

        ready = user['state'] == 'active'

        await ws.send_str(str(int(ready)))
        return ws
    finally:
        await ws.close()


@routes.get('/signup')
async def signup(request):
    next_page = request.query.get('next', deploy_config.external_url('auth', '/user'))

    flow_data = request.app['flow_client'].initiate_flow(deploy_config.external_url('auth', '/oauth2callback'))

    session = await aiohttp_session.new_session(request)
    cleanup_session(session)
    session['next'] = next_page
    session['caller'] = 'signup'
    session['flow'] = flow_data

    return aiohttp.web.HTTPFound(flow_data['authorization_url'])


@routes.get('/login')
async def login(request):
    next_page = request.query.get('next', deploy_config.external_url('auth', '/user'))

    flow_data = request.app['flow_client'].initiate_flow(deploy_config.external_url('auth', '/oauth2callback'))

    session = await aiohttp_session.new_session(request)
    cleanup_session(session)
    session['next'] = next_page
    session['caller'] = 'login'
    session['flow'] = flow_data

    return aiohttp.web.HTTPFound(flow_data['authorization_url'])


@routes.get('/oauth2callback')
async def callback(request):
    session = await aiohttp_session.get_session(request)
    if 'flow' not in session:
        raise web.HTTPUnauthorized()

    next_url = deploy_config.external_url('auth', '/user')
    creating_url = deploy_config.external_url('auth', '/creating')

    caller = session['caller']
    next_page = session.pop('next', next_url)
    flow_dict = session['flow']
    flow_dict['callback_uri'] = deploy_config.external_url('auth', '/oauth2callback')
    cleanup_session(session)

    try:
        flow_result = request.app['flow_client'].receive_callback(request, flow_dict)
        login_id = flow_result.login_id
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.exception('oauth2 callback: could not fetch and verify token')
        raise web.HTTPUnauthorized() from e

    db = request.app['db']

    user = await user_from_login_id(db, login_id)

    if user is None:
        if caller == 'login':
            set_message(session, f'Account does not exist for login id {login_id}', 'error')
            return aiohttp.web.HTTPFound(deploy_config.external_url('auth', ''))

        assert caller == 'signup'

        username, domain = flow_result.email.split('@')
        username = ''.join(c for c in username if c.isalnum())

        if domain != ORGANIZATION_DOMAIN:
            raise web.HTTPUnauthorized()

        try:
            await insert_new_user(db, username, login_id, is_developer=False, is_service_account=False)
        except AuthUserError as e:
            set_message(session, e.message, 'error')
            return web.HTTPFound(deploy_config.external_url('auth', ''))

        session['pending'] = True
        session['login_id'] = login_id

        return web.HTTPFound(creating_url)

    if user['state'] in ('deleting', 'deleted'):
        page_context = {'username': user['username'], 'state': user['state'], 'login_id': user['login_id']}
        return await render_template('auth', request, user, 'account-error.html', page_context)

    if user['state'] == 'creating':
        if caller == 'signup':
            set_message(session, f'Account is already creating for login id {login_id}', 'error')
        if caller == 'login':
            set_message(session, f'Account for login id {login_id} is still being created.', 'error')
        session['pending'] = True
        session['login_id'] = user['login_id']
        return web.HTTPFound(creating_url)

    assert user['state'] == 'active'
    if caller == 'signup':
        set_message(session, f'Account has already been created for {user["username"]}.', 'info')
    session_id = await create_session(db, user['id'])
    session['session_id'] = session_id
    return aiohttp.web.HTTPFound(next_page)


@routes.post('/api/v1alpha/users/{user}/create')
@auth.rest_authenticated_developers_only
async def create_user(request: web.Request, userdata):  # pylint: disable=unused-argument
    db: Database = request.app['db']
    username = request.match_info['user']

    body = await request.json()
    login_id = body['login_id']
    is_developer = body['is_developer']
    is_service_account = body['is_service_account']

    try:
        await insert_new_user(db, username, login_id, is_developer, is_service_account)
    except AuthUserError as e:
        raise e.http_response()

    return web.json_response()


@routes.get('/user')
@auth.web_authenticated_users_only()
async def user_page(request, userdata):
    return await render_template('auth', request, userdata, 'user.html', {'cloud': CLOUD})


async def create_copy_paste_token(db, session_id, max_age_secs=300):
    copy_paste_token = secret_alnum_string()
    await db.just_execute(
        "INSERT INTO copy_paste_tokens (id, session_id, max_age_secs) VALUES(%s, %s, %s);",
        (copy_paste_token, session_id, max_age_secs),
    )
    return copy_paste_token


@routes.post('/copy-paste-token')
@check_csrf_token
@auth.web_authenticated_users_only()
async def get_copy_paste_token(request, userdata):
    session = await aiohttp_session.get_session(request)
    session_id = session['session_id']
    db = request.app['db']
    copy_paste_token = await create_copy_paste_token(db, session_id)
    page_context = {'copy_paste_token': copy_paste_token}
    return await render_template('auth', request, userdata, 'copy-paste-token.html', page_context)


@routes.post('/api/v1alpha/copy-paste-token')
@auth.rest_authenticated_users_only
async def get_copy_paste_token_api(request, userdata):
    session_id = userdata['session_id']
    db = request.app['db']
    copy_paste_token = await create_copy_paste_token(db, session_id)
    return web.Response(body=copy_paste_token)


@routes.post('/logout')
@check_csrf_token
@auth.web_maybe_authenticated_user
async def logout(request, userdata):
    if not userdata:
        return web.HTTPFound(deploy_config.external_url('auth', ''))

    db = request.app['db']
    session_id = userdata['session_id']
    await db.just_execute('DELETE FROM sessions WHERE session_id = %s;', session_id)

    session = await aiohttp_session.get_session(request)
    cleanup_session(session)

    return web.HTTPFound(deploy_config.external_url('auth', ''))


@routes.get('/api/v1alpha/login')
async def rest_login(request):
    callback_port = request.query['callback_port']
    callback_uri = f'http://127.0.0.1:{callback_port}/oauth2callback'
    flow_data = request.app['flow_client'].initiate_flow(callback_uri)
    flow_data['callback_uri'] = callback_uri

    # keeping authorization_url and state for backwards compatibility
    return web.json_response(
        {'flow': flow_data, 'authorization_url': flow_data['authorization_url'], 'state': flow_data['state']}
    )


@routes.get('/roles')
@auth.web_authenticated_developers_only()
async def get_roles(request, userdata):
    db = request.app['db']
    roles = [x async for x in db.select_and_fetchall('SELECT * FROM roles;')]
    page_context = {'roles': roles}
    return await render_template('auth', request, userdata, 'roles.html', page_context)


@routes.post('/roles')
@check_csrf_token
@auth.web_authenticated_developers_only()
async def post_create_role(request, userdata):  # pylint: disable=unused-argument
    session = await aiohttp_session.get_session(request)
    db = request.app['db']
    post = await request.post()
    name = post['name']

    role_id = await db.execute_insertone(
        '''
INSERT INTO `roles` (`name`)
VALUES (%s);
''',
        (name),
    )

    set_message(session, f'Created role {role_id} {name}.', 'info')

    return web.HTTPFound(deploy_config.external_url('auth', '/roles'))


@routes.get('/users')
@auth.web_authenticated_developers_only()
async def get_users(request, userdata):
    db = request.app['db']
    users = [x async for x in db.select_and_fetchall('SELECT * FROM users;')]
    page_context = {'users': users}
    return await render_template('auth', request, userdata, 'users.html', page_context)


@routes.post('/users')
@check_csrf_token
@auth.web_authenticated_developers_only()
async def post_create_user(request, userdata):  # pylint: disable=unused-argument
    session = await aiohttp_session.get_session(request)
    db = request.app['db']
    post = await request.post()
    username = post['username']
    login_id = post.get('login_id', '')
    is_developer = post.get('is_developer') == '1'
    is_service_account = post.get('is_service_account') == '1'

    try:
        if login_id == '':
            login_id = None
        created_user = await insert_new_user(db, username, login_id, is_developer, is_service_account)
    except AuthUserError as e:
        set_message(session, e.message, 'error')
        return web.HTTPFound(deploy_config.external_url('auth', '/users'))

    if created_user:
        set_message(session, f'Created user {username} {login_id}.', 'info')
    else:
        set_message(session, f'User {username} {login_id} already exists.', 'info')

    return web.HTTPFound(deploy_config.external_url('auth', '/users'))


@routes.get('/api/v1alpha/users')
@auth.rest_authenticated_developers_only
async def rest_get_users(request, userdata):  # pylint: disable=unused-argument
    db: Database = request.app['db']
    users = await db.select_and_fetchall(
        '''
SELECT id, username, login_id, state, is_developer, is_service_account FROM users;
'''
    )
    return web.json_response([user async for user in users])


@routes.get('/api/v1alpha/users/{user}')
@auth.rest_authenticated_developers_only
async def rest_get_user(request, userdata):  # pylint: disable=unused-argument
    db: Database = request.app['db']
    username = request.match_info['user']

    user = await db.select_and_fetchone(
        '''
SELECT id, username, login_id, state, is_developer, is_service_account FROM users
WHERE username = %s;
''',
        (username,),
    )
    if user is None:
        raise web.HTTPNotFound()
    return web.json_response(user)


async def _delete_user(db: Database, username: str, id: Optional[str]):
    where_conditions = ['state != "deleted"', 'username = %s']
    where_args = [username]

    if id is not None:
        where_conditions.append('id = %s')
        where_args.append(id)

    n_rows = await db.execute_update(
        f'''
UPDATE users
SET state = 'deleting'
WHERE {' AND '.join(where_conditions)};
''',
        where_args,
    )

    if n_rows == 0:
        raise UnknownUser(username)


@routes.post('/users/delete')
@check_csrf_token
@auth.web_authenticated_developers_only()
async def delete_user(request, userdata):  # pylint: disable=unused-argument
    session = await aiohttp_session.get_session(request)
    db = request.app['db']
    post = await request.post()
    id = post['id']
    username = post['username']

    try:
        await _delete_user(db, username, id)
        set_message(session, f'Deleted user {id} {username}.', 'info')
    except UnknownUser:
        set_message(session, f'Delete failed, no such user {id} {username}.', 'error')

    return web.HTTPFound(deploy_config.external_url('auth', '/users'))


@routes.delete('/api/v1alpha/users/{user}')
@auth.rest_authenticated_developers_only
async def rest_delete_user(request: web.Request, userdata):  # pylint: disable=unused-argument
    db = request.app['db']
    username = request.match_info['user']

    try:
        await _delete_user(db, username, None)
    except UnknownUser as e:
        return e.http_response()

    return web.json_response()


@routes.get('/api/v1alpha/oauth2callback')
async def rest_callback(request):
    flow_json = request.query.get('flow')
    if flow_json is None:
        # backwards compatibility with older versions of hailctl
        callback_port = request.query['callback_port']
        flow_dict = {
            'state': request.query['state'],
            'callback_uri': f'http://127.0.0.1:{callback_port}/oauth2callback',
        }
    else:
        flow_dict = json.loads(request.query['flow'])

    try:
        flow_result = request.app['flow_client'].receive_callback(request, flow_dict)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.exception('fetching and decoding token')
        raise web.HTTPUnauthorized() from e

    db = request.app['db']
    users = [
        x
        async for x in db.select_and_fetchall(
            "SELECT * FROM users WHERE login_id = %s AND state = 'active';", flow_result.login_id
        )
    ]

    if len(users) != 1:
        raise web.HTTPUnauthorized()
    user = users[0]

    session_id = await create_session(db, user['id'], max_age_secs=None)

    return web.json_response({'token': session_id, 'username': user['username']})


@routes.post('/api/v1alpha/copy-paste-login')
async def rest_copy_paste_login(request):
    copy_paste_token = request.query['copy_paste_token']
    db = request.app['db']

    @transaction(db)
    async def maybe_pop_token(tx):
        session = await tx.execute_and_fetchone(
            """
SELECT sessions.session_id AS session_id, users.username AS username FROM copy_paste_tokens
INNER JOIN sessions ON sessions.session_id = copy_paste_tokens.session_id
INNER JOIN users ON users.id = sessions.user_id
WHERE copy_paste_tokens.id = %s
  AND NOW() < TIMESTAMPADD(SECOND, copy_paste_tokens.max_age_secs, copy_paste_tokens.created)
  AND users.state = 'active';""",
            copy_paste_token,
        )
        if session is None:
            raise web.HTTPUnauthorized()
        await tx.just_execute("DELETE FROM copy_paste_tokens WHERE id = %s;", copy_paste_token)
        return session

    session = await maybe_pop_token()  # pylint: disable=no-value-for-parameter
    return web.json_response({'token': session['session_id'], 'username': session['username']})


@routes.post('/api/v1alpha/logout')
@auth.rest_authenticated_users_only
async def rest_logout(request, userdata):
    session_id = userdata['session_id']
    db = request.app['db']
    await db.just_execute('DELETE FROM sessions WHERE session_id = %s;', session_id)

    return web.Response(status=200)


async def get_userinfo(request, session_id):
    # b64 encoding of 32-byte session ID is 44 bytes
    if len(session_id) != 44:
        log.info('Session id != 44 bytes')
        raise web.HTTPUnauthorized()

    db = request.app['db']
    users = [
        x
        async for x in db.select_and_fetchall(
            '''
SELECT users.*, sessions.session_id FROM users
INNER JOIN sessions ON users.id = sessions.user_id
WHERE users.state = 'active' AND (sessions.session_id = %s) AND (ISNULL(sessions.max_age_secs) OR (NOW() < TIMESTAMPADD(SECOND, sessions.max_age_secs, sessions.created)));
''',
            session_id,
            'get_userinfo',
        )
    ]

    if len(users) != 1:
        log.info(f'Unknown session id: {session_id}')
        raise web.HTTPUnauthorized()
    return users[0]


@routes.get('/api/v1alpha/userinfo')
async def userinfo(request):
    if 'Authorization' not in request.headers:
        log.info('Authorization not in request.headers')
        raise web.HTTPUnauthorized()

    auth_header = request.headers['Authorization']
    session_id = maybe_parse_bearer_header(auth_header)
    if not session_id:
        log.info('Bearer not in Authorization header')
        raise web.HTTPUnauthorized()

    return web.json_response(await get_userinfo(request, session_id))


async def get_session_id(request):
    if 'X-Hail-Internal-Authorization' in request.headers:
        return maybe_parse_bearer_header(request.headers['X-Hail-Internal-Authorization'])

    if 'Authorization' in request.headers:
        return maybe_parse_bearer_header(request.headers['Authorization'])

    session = await aiohttp_session.get_session(request)
    return session.get('session_id')


@routes.route('*', '/api/v1alpha/verify_dev_credentials')
async def verify_dev_credentials(request):
    session_id = await get_session_id(request)
    if not session_id:
        raise web.HTTPUnauthorized()
    userdata = await get_userinfo(request, session_id)
    is_developer = userdata is not None and userdata['is_developer'] == 1
    if not is_developer:
        raise web.HTTPUnauthorized()
    return web.Response(status=200)


@routes.route('*', '/api/v1alpha/verify_dev_or_sa_credentials')
async def verify_dev_or_sa_credentials(request):
    session_id = await get_session_id(request)
    if not session_id:
        raise web.HTTPUnauthorized()
    userdata = await get_userinfo(request, session_id)
    is_developer_or_sa = userdata is not None and (userdata['is_developer'] == 1 or userdata['is_service_account'] == 1)
    if not is_developer_or_sa:
        raise web.HTTPUnauthorized()
    return web.Response(status=200)


async def on_startup(app):
    db = Database()
    await db.async_init(maxsize=50)
    app['db'] = db
    app['client_session'] = httpx.client_session()
    app['flow_client'] = get_flow_client('/auth-oauth2-client-secret/client_secret.json')


async def on_cleanup(app):
    try:
        await app['db'].async_close()
    finally:
        await app['client_session'].close()


class AuthAccessLogger(AccessLogger):
    def __init__(self, logger: logging.Logger, log_format: str):
        super().__init__(logger, log_format)
        self.exclude = [
            (endpoint[0], re.compile(deploy_config.base_path('auth') + endpoint[1]))
            for endpoint in [
                ('GET', '/api/v1alpha/userinfo'),
            ]
        ]

    def log(self, request, response, time):
        for method, path_expr in self.exclude:
            if path_expr.fullmatch(request.path) and method == request.method:
                return

        super().log(request, response, time)


def run():
    app = web.Application(middlewares=[monitor_endpoints_middleware])

    setup_aiohttp_jinja2(app, 'auth')
    setup_aiohttp_session(app)

    setup_common_static_routes(routes)
    app.add_routes(routes)
    app.router.add_get("/metrics", server_stats)

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)

    web.run_app(
        deploy_config.prefix_application(app, 'auth'),
        host='0.0.0.0',
        port=443,
        access_log_class=AuthAccessLogger,
        ssl_context=internal_server_ssl_context(),
    )
