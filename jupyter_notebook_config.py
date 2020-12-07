c = get_config()
c.NotebookApp.ip = '0.0.0.0'
c.NotebookApp.open_browser = False
c.NotebookApp.port = 9999

# setting up the password
from IPython.lib import passwd
password = passwd("fluid_framework!?")
c.NotebookApp.password = password
