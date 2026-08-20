import os
import threading
import time
import requests
import typer
from tinydb import TinyDB

ACCLI_DEBUG = os.environ.get('ACCLI_DEBUG', False)

_token_lock = threading.Lock()
_cached_cas_token = None
_cached_expires_at = 0
_cached_access_token = None


def get_db_path():
    sudo_user = os.environ.get("SUDO_USER")
    pkexec_uid = os.environ.get("PKEXEC_UID")

    if (sudo_user or pkexec_uid) and os.name != "nt":
        try:
            import pwd
            if pkexec_uid:
                home = pwd.getpwuid(int(pkexec_uid)).pw_dir
            else:
                home = pwd.getpwnam(sudo_user).pw_dir
        except Exception:
            home = os.path.expanduser("~")
    else:
        home = os.path.expanduser("~")

    token_directory = f"{home}/.accli"

    if not os.path.exists(token_directory):
        os.makedirs(token_directory)

    return f"{token_directory}/data.json"


def save_token_details(token, server_url, webcli_url):
    db_path = get_db_path()
    db = TinyDB(db_path)

    if len(db) > 0:
        doc = next(iter(db))
        doc_id = doc.doc_id
        db.update({
            'token': token,
            'server_url': server_url,
            'webcli_url': webcli_url
        }, doc_ids=[doc_id])
        for extra in list(db):
            if extra.doc_id != doc_id:
                db.remove(doc_ids=[extra.doc_id])
    else:
        db.insert({
            'token': token,
            'server_url': server_url,
            'webcli_url': webcli_url
        })


def get_token():
    db_path = get_db_path()

    db = TinyDB(db_path)

    for item in db:
        token = item.get('token')
        if token:
            break

    if not token:
        print("Token does not exists. Please login.")
    return token


def exchange_refresh_token(project_slug: str) -> tuple[str, str, int]:
    """
    Exchanges the stored refresh token for a short-lived access token
    and a new rotated refresh token. Updates the local TinyDB token cache.
    Returns (cas_token, access_token, expires_at).
    """
    global _cached_cas_token, _cached_expires_at, _cached_access_token

    # 1. Fast-path check: Reuse valid in-memory cache if still fresh (> 5 mins remaining)
    # and matches the requested project slug prefix.
    now = int(time.time())
    if _cached_cas_token and _cached_access_token and (_cached_expires_at - now > 300):
        expected_prefix = f"xet_session_prj_{project_slug}_"
        if _cached_cas_token.startswith(expected_prefix):
            return _cached_cas_token, _cached_access_token, _cached_expires_at

    # 2. Block/lock to serialize requests and avoid rotation race conditions (RTR invalidation)
    with _token_lock:
        # Re-check cache inside the lock (double-checked locking pattern)
        now = int(time.time())
        if _cached_cas_token and _cached_access_token and (_cached_expires_at - now > 300):
            expected_prefix = f"xet_session_prj_{project_slug}_"
            if _cached_cas_token.startswith(expected_prefix):
                return _cached_cas_token, _cached_access_token, _cached_expires_at

        db_path = get_db_path()
        db = TinyDB(db_path)
        item = next(iter(db), {})
        refresh_token = item.get('token')
        server_url = item.get('server_url', "https://accelerator.iiasa.ac.at")
        webcli_url = item.get('webcli_url', "https://accelerator.iiasa.ac.at")

        if not refresh_token:
            print("[bold red]ERROR: No token found. Please run 'accli login' first.[/bold red]")
            raise typer.Exit(1)

        refresh_endpoint = f"{server_url.rstrip('/')}/api/v1/oauth/device/access-token/"

        try:
            response = requests.post(
                refresh_endpoint,
                json={"refresh_token": refresh_token},
                verify=(not ACCLI_DEBUG)
            )
            response.raise_for_status()
            data = response.json()

            access_token = data["access_token"]
            new_refresh_token = data["refresh_token"]

            # Save rotated refresh token back to local TinyDB
            save_token_details(new_refresh_token, server_url, webcli_url)

            cas_token = f"xet_session_prj_{project_slug}_{access_token}"
            # Expire slightly before the 1-hour limit (e.g. 50 minutes)
            expires_at = int(time.time()) + 3000

            # Populate in-memory cache
            _cached_cas_token = cas_token
            _cached_access_token = access_token
            _cached_expires_at = expires_at

            return cas_token, access_token, expires_at
        except Exception as e:
            print(f"[bold red]ERROR: Failed to authenticate/exchange refresh token: {e}[/bold red]")
            raise typer.Exit(1)


def get_github_app_token():
    db_path = get_db_path()

    db = TinyDB(db_path)

    for item in db:
        token = item.get('github_app_token')
        if token:
            break

    if not token:
        print("Github app token does not exists.")
    return token


def set_github_app_token(github_app_token):
    db_path = get_db_path()
    db = TinyDB(db_path)
    if len(db) > 0:
        doc = next(iter(db))
        db.update({'github_app_token': github_app_token}, doc_ids=[doc.doc_id])
    else:
        db.insert({'github_app_token': github_app_token})


def set_project_slug(project_slug):
    db_path = get_db_path()
    db = TinyDB(db_path)
    if len(db) > 0:
        doc = next(iter(db))
        db.update({'project_slug': project_slug}, doc_ids=[doc.doc_id])
    else:
        db.insert({'project_slug': project_slug})


def get_project_slug():
    db_path = get_db_path()

    db = TinyDB(db_path)

    for item in db:
        project_slug = item.get('project_slug')
        if project_slug:
            break

    if not project_slug:
        print("project slug was not set.")
    return project_slug


def get_server_url():
    db_path = get_db_path()

    db = TinyDB(db_path)

    for item in db:
        server_url = item.get('server_url')
        if server_url:
            break

    if not server_url:
        print("Server url does not exists. Please login.")
    return server_url

