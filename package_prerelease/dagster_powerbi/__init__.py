from dagster._core.libraries import DagsterLibraryRegistry

from dagster_powerbi.resource import PowerBIWorkspace as PowerBIWorkspace
from dagster_powerbi.translator import DagsterPowerBITranslator as DagsterPowerBITranslator

# Move back to version.py and edit setup.py once we are ready to publish.
__version__ = "0.0.6"

DagsterLibraryRegistry.register("dagster-powerbi", __version__)
