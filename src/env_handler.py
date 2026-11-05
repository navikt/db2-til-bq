import os
import shutil

from dataclasses import dataclass, field
from dotenv import load_dotenv
from pathlib import Path

from src.exceptions import EnvsNotSetError, Db2LicenseNotFoundError


@dataclass
class EnvHandler:
    """Laster inn og kontrollerer miljøvariabler for en datakilde.

    Attributes
    ----------
    source_name : str
        Navnet som brukes til å finne lokal miljøfil.
    required_envs : list of str
        Miljøvariablene som skal kontrolleres.
    local : bool
        Om prosessen kjøres lokalt, bestemt ut fra ``NAIS_CLUSTER_NAME``.
    """

    source_name: str
    required_envs: list[str] = field(init=False)
    local: bool = field(init=False)

    def __post_init__(self) -> None:
        """Finn påkrevde variabler, bestem kjøremiljø og håndter DB2-lisensen."""
        self.required_envs = self._get_required_envs()
        self.local = self._set_local()
        self._copy_db2_license()

    def __iter__(self):
        """Iterer over navnene på de påkrevde miljøvariablene.

        Yields
        ------
        str
            Navnet på neste påkrevde miljøvariabel.
        """
        for required_env in self.required_envs:
            yield required_env

    def load_envs(self) -> None:
        """Last inn ``<source_name>.env`` når prosessen kjører lokalt."""
        if self.local:
            load_dotenv(f"{self.source_name}.env")

    def check_envs(self) -> None:
        """Kontroller at alle påkrevde miljøvariabler finnes.

        Raises
        ------
        EnvsNotSetError
            Hvis én eller flere påkrevde miljøvariabler mangler.
        """
        missing_envs = []
        for required_env in self.required_envs:
            try:
                os.environ[required_env]
            except KeyError:
                missing_envs.append(required_env)

        if missing_envs:
            raise EnvsNotSetError(
                message=f"Missing required environment variables: {missing_envs}"
            )

    def _copy_db2_license(self):
        """Kopier DB2-lisensen i ikke-lokalt miljø.

        Raises
        ------
        Db2LicenseNotFoundError
            Hvis lisensfilen ikke finnes på den forventede kildebanen.
        """
        if not self.local:
            license_source = Path("/var/run/secrets/db2-license/db2consv_zs.lic")
            license_destination = Path(
                "/app/venv/lib/python3.13/site-packages/clidriver/license/db2consv_zs.lic"
            )

            if license_source.exists():
                resolved_source = license_source.resolve()
                shutil.copy2(resolved_source, license_destination)

            else:
                raise Db2LicenseNotFoundError(
                    f"Db2 license not found at {license_source}"
                )

    @staticmethod
    def _get_required_envs() -> list[str]:
        """Returner navnene på miljøvariablene som kreves av programmet.

        Returns
        -------
        list of str
            Påkrevde miljøvariabelnavn.
        """
        return [
            "DATABASE_USERNAME",
            "DATABASE_PASSWORD",
            "DATABASE_SCHEMA",
            "DATABASE_NAME",
            "DATABASE_PORT",
            "DATABASE_HOST",
            "GOOGLE_CLOUD_PROJECT",
        ]

    @staticmethod
    def _set_local() -> bool:
        """Avgjør om prosessen kjører lokalt ut fra ``NAIS_CLUSTER_NAME``.

        Returns
        -------
        bool
            ``True`` når miljøvariabelen ikke er satt.
        """
        return os.environ.get("NAIS_CLUSTER_NAME") is None
