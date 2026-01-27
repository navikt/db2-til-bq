import os
import shutil

from dataclasses import dataclass, field
from dotenv import load_dotenv
from pathlib import Path

from src.exceptions import EnvsNotSetError, Db2LicenseNotFoundError


@dataclass
class EnvHandler:
    required_envs: list[str] = field(init=False)
    local: bool = field(init=False)

    def __post_init__(self) -> None:
        self.required_envs = self._get_required_envs()
        self.local = self._set_local()
        self._copy_db2_license()

    def __iter__(self):
        for required_env in self.required_envs:
            yield required_env


    def load_envs(self) -> None:
        if self.local:
            load_dotenv()

    def check_envs(self) -> None:
        missing_envs = []
        for required_env in self.required_envs:
            try:
                os.environ[required_env]
            except KeyError:
                missing_envs.append(required_env)

        if missing_envs:
            raise EnvsNotSetError(message=f"Missing required environment variables: {missing_envs}")

    def _copy_db2_license(self):
        if not self.local:
            license_source = Path("/var/run/secrets/db2-license/db2consv_zs.lic")
            license_destination = Path("/app/venv/lib/python3.13/site-packages/clidriver/license/db2consv_zs.lic" )

            if license_source.exists():
                resolved_source = license_source.resolve()
                shutil.copy2(resolved_source, license_destination)
            else:
                raise Db2LicenseNotFoundError(f"Db2 license not found at {license_source}")

    @staticmethod
    def _get_required_envs() -> list[str]:
        return [
            "DATABASE_USERNAME",
            "DATABASE_PASSWORD",
            "DATABASE_SCHEMA",
            "DATABASE_NAME",
            "DATABASE_PORT",
            "DATABASE_HOST",
            "GOOGLE_CLOUD_PROJECT"]

    @staticmethod
    def _set_local() -> bool:
        return os.environ.get("NAIS_CLUSTER_NAME") is None

