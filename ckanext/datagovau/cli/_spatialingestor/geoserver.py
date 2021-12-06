from __future__ import annotations

import re
import os
from typing import Any, NamedTuple

import requests

from .exc import BadConfig


class GeoServer(NamedTuple):
    host: str
    user: str
    password: str
    public_url: str

    def into_workspace(self, raw: str):
        if any([c.isalpha() for c in raw]):
            if not raw[0].isalpha():
                raw += "-"
                while not raw[0].isalpha():
                    first_literal = raw[0]
                    raw = raw[1:]
                    if first_literal.isdigit():
                        raw += first_literal
                if raw[-1] == "-":
                    raw = raw[:-1]
        else:
            raw = "ckan-" + raw

        return raw

    def check_workspace(self, workspace: str) -> bool:
        url = self._workspace_url(workspace)
        with self._session() as s:
            return s.head(url).ok

    def drop_workspace(self, workspace: str):
        url = self._workspace_url(workspace)
        with self._session() as s:
            return s.delete(url + "?recurse=true&quietOnNotFound")

    def create_workspace(self, workspace: str):
        url = self._workspace_url()
        with self._session() as s:
            return s.post(
                url,
                json={"workspace": {"name": workspace}},
            )

    def create_store(self, workspace: str, is_cs: bool, data: dict[str, Any]):
        url = self._store_url(workspace, is_cs)
        with self._session() as s:
            # POST creates, PUT updates
            return s.post(url, json=data)

    def create_layer(
        self, workspace: str, is_cs: bool, store: str, data: dict[str, Any]
    ):
        url = self._layer_url(workspace, is_cs, store)
        with self._session() as s:
            # POST creates, PUT updates
            return s.post(url, json=data)

    def get_style(self, workspace: str, style: str, quiet: bool = False):
        url = self._style_url(workspace, style)
        params = {}
        if quiet:
            params["quietOnNotFound"] = True
        with self._session() as s:
            return s.get(url, params=params)

    def create_style(self, workspace: str, data: dict[str, Any]):
        url = self._style_url(workspace)
        with self._session() as s:
            return s.post(url, json=data)

    def delete_style(self, workspace: str, style: str):
        url = self._style_url(workspace, style)
        with self._session() as s:
            return s.delete(url)

    def update_style(
        self,
        workspace: str,
        style: str,
        data: Any,
        content_type: str,
        raw: bool,
    ):
        url = self._style_url(workspace, style)
        with self._session() as s:
            return s.put(
                url,
                data=data,
                headers={"Content-type": content_type},
                params={"raw": raw},
            )

    def add_style(
        self, workspace: str, layer: str, style: str, data: dict[str, Any]
    ):
        url = f"{self.host}rest/layers/{layer}"
        with self._session() as s:
            return s.put(url, json=data)

    def _workspace_url(self, workspace: str = "") -> str:
        return f"{self.host}rest/workspaces/{workspace}"

    def _store_url(self, workspace: str, is_cs: bool, store: str = "") -> str:
        type_ = "coveragestores" if is_cs else "datastores"
        base = self._workspace_url(workspace)

        return f"{base}/{type_}/{store}"

    def _style_url(self, workspace: str, style: str = "") -> str:
        base = self._workspace_url(workspace)
        return f"{base}/styles/{style}"

    def _layer_url(self, workspace: str, is_cs: bool, store: str) -> str:
        type_ = "coveragestores" if is_cs else "datastores"
        sub_type = "coverages" if is_cs else "featuretypes"
        base = self._workspace_url(workspace)

        return f"{base}/{type_}/{store}/{sub_type}"

    def _session(self):
        session = requests.Session()
        session.auth = (self.user, self.password)
        return session


def get_geoserver() -> GeoServer:
    regex = [
        r"^\s*(?P<db_type>\w*)",
        "://",
        "(?P<db_user>[^:]*)",
        ":?",
        "(?P<db_pass>.*)",
        "@",
        "(?P<db_host>[^/:]*)",
        ":?",
        "(?P<db_port>[^/]*)",
        "/",
        r"(?P<db_name>[\w.-]*)",
    ]
    admin_url = os.environ["GEOSERVER_ADMIN_URL"]
    public_url = os.environ["CKAN_SITE_URL"] + "/geoserver"

    match = re.match("".join(regex), admin_url)

    if not match:
        raise BadConfig(f"Invalid GEOSERVER_ADMIN_URL: {admin_url}")

    info = match.groupdict()
    host = "https://" + info["db_host"]

    port = info.get("db_port", "")
    if port:
        host += ":" + port

    host += "/" + info["db_name"] + "/"
    return GeoServer(host, info["db_user"], info["db_pass"], public_url)
