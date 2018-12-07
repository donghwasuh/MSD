import M6.Common.Server.TCPThreadServer as TCPThreadServer

from M6.Master.MSD import MSD
from M6.Common.DB import Backend
import M6.Common.Default as Default


from Mobigen.Common import Log
from Mobigen.Common.Log import __LOG__
Log.Init()


def Main(argv) :

    logFile = "%s/MSD.log" % (Default.M6_LOG_DIR)
    # FIXME : ADD MSD log option to Default 
    #Log.Init( Log.CRotatingLog (logFile, Default.MSD_LOG_MAX_LINE, Default.MSD_LOG_MAX_FILE))
    Log.Init( Log.CRotatingLog (logFile, Default.BIM_LOG_MAX_LINE, Default.BIM_LOG_MAX_FILE))

    server = TCPThreadServer.Server( 9999 , MSD.MSD, None )
    #__LOG__.Trace("Start BSD (port=%s)" % Default.PORT["BMD"])
    __LOG__.Trace("Start MSD (port=9999)")
    server.start()

if __name__ == '__main__':
    Main(None)
