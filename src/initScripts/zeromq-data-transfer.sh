#!/bin/sh

# LSB compliant init-script header.
### BEGIN INIT INFO
# Provides:          zeromq-data-transfer
# Required-Start:    $syslog networking
# Required-Stop:     $syslog networking
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Start the ZMQ data transfer
### END INIT INFO


# PATH should only include /usr/* if it runs after the mountnfs.sh script
PATH=/sbin:/usr/sbin:/bin:/usr/bin
DESC="ZMQ data transfer"
# Process name ( For display )
NAME=zeromq-data-transfer
#DAEMON=/home/kuhnm/Arbeit/zeromq-data-transfer/src/sender/DataManager.py
DAEMON=/space/projects/zeromq-data-transfer/src/sender/DataManager.py
DAEMON_ARGS="--verbose"
#PIDFILE=/home/kuhnm/Arbeit/zeromq-data-transfer/$NAME.pid
PIDFILE=/space/projects/zeromq-data-transfer/$NAME.pid
PYTHON=/usr/bin/python

SCRIPTNAME=/etc/init.d/$NAME

if [ -f /etc/redhat-release -o -f /etc/centos-release ] ; then
# Red Hat or Centos...

    # source function library.
    . /etc/rc.d/init.d/functions

    RETVAL=0
    start()
    {
    	echo -n "Starting ${DESC}..."
	    ${DAEMON} ${DAEMON_ARGS} &
#	    /usr/bin/python ${DAEMON} --pidfile ${PIDFILE}
    	RETVAL=$?
#	    [ "$RETVAL" = 0 ] && touch /var/lock/subsys/zeromq-data-transfer
    	echo
    }

    stop()
    {
	    echo -n "Stopping ${DESC}..."
        DATATRANSFER_PID="`pidofproc ${NAME}`"
        # stop gracefully and wait up to 180 seconds.
#        if [ -z "$DATATRANSFER_PID" ]; then
#            exit 0
#        else
            kill $DATATRANSFER_PID > /dev/null 2>&1
#        fi

        TIMEOUT=0
        while checkpid $DATATRANSFER_PID && [ $TIMEOUT -lt 30 ] ; do
            sleep 1
            let TIMEOUT=TIMEOUT+1
        done
        if checkpid $DATATRANSFER_PID ; then
            kill -KILL $DATATRANSFER_PID
            #TODO rm ipc sockets in /tmp/zeromq-data-transfer
        fi
    	RETVAL=$?
#    	[ "$RETVAL" = 0 ] && rm -f /var/lock/subsys/zeromq-data-transfer
	    echo
    }

    case "$1" in
        start)
            start
            ;;
        stop)
            stop
            ;;
        restart)
            echo -n "Restarting ${DESC}: "
            stop
            start
            ;;
        status)
            status ${NAME}
#            status -p ${PIDFILE} ${NAME}
            RETVAL=$?
            ;;
        *)
            echo "Usage: $0 {start|stop|status|restart}"
            RETVAL=1
            ;;
    esac
    exit $RETVAL

elif [ -f /etc/debian_version ] ; then
# Debian and Ubuntu

## Exit if the package is not installed
#    [ -x "$DAEMON" ] || exit 0

# Load the VERBOSE setting and other rcS variables
#    . /lib/init/vars.sh

# Define LSB log_* functions.
# Depend on lsb-base (>= 3.2-14) to ensure that this file is present
# and status_of_proc is working.
    . /lib/lsb/init-functions

#    ln -s "$DAEMON" "$NAME"
#
# Function that starts the daemon/service
#
    do_start()
    {
        # Return
        #   0 if daemon has been started
        #   1 if daemon was already running
        #   2 if daemon could not be started
        start-stop-daemon --start --quiet --pidfile $PIDFILE --make-pidfile --exec $DAEMON --test > /dev/null \
            || return 1

#        start-stop-daemon --start --quiet --pidfile $PIDFILE --make-pidfile \
        start-stop-daemon --start --quiet --pidfile $PIDFILE --make-pidfile --background \
            --startas $DAEMON -- $DAEMON_ARGS \
            || return 2
    }

#
# Function that stops the daemon/service
#
    do_stop()
    {
        # Return
        #   0 if daemon has been stopped
        #   1 if daemon was already stopped
        #   2 if daemon could not be stopped
        #   other if a failure occurred
        start-stop-daemon --stop --quiet --pidfile $PIDFILE #--name $NAME
#        start-stop-daemon --stop --quiet --retry=TERM/180/KILL/5 --pidfile $PIDFILE

#        pidofproc $NAME || status="$?";

        RETVAL="$?"
        [ "$RETVAL" = 2 ] && return 2
        # Wait for children to finish too if this is a daemon that forks
        # and if the daemon is only ever run from this initscript.
        # If the above conditions are not satisfied then add some other code
        # that waits for the process to drop all resources that could be
        # needed by services started subsequently.  A last resort is to
        # sleep for some time.
        start-stop-daemon --stop --quiet --oknodo --retry=0/30/KILL/5 --exec $DAEMON
        [ "$?" = 2 ] && return 2
        # Many daemons don't delete their pidfiles when they exit.
        rm -f $PIDFILE
        return "$RETVAL"
    }

#
# Function that sends a SIGHUP to the daemon/service
#
    do_reload()
    {
        # If the daemon can reload its configuration without
        # restarting (for example, when it is sent a SIGHUP),
        # then implement that here.
        start-stop-daemon --stop --signal 1 --quiet --pidfile $PIDFILE --name $NAME
        return 0
    }

    case "$1" in
        start)
            [ "$VERBOSE" != no ] && log_daemon_msg "Starting $DESC" "$NAME"
            do_start
            case "$?" in
                0|1) [ "$VERBOSE" != no ] && log_end_msg 0
                    ;;
                2) [ "$VERBOSE" != no ] && log_end_msg 1
                    ;;
            esac
            ;;
        stop)
            [ "$VERBOSE" != no ] && log_daemon_msg "Stopping $DESC" "$NAME"
            do_stop
            case "$?" in
                0|1) [ "$VERBOSE" != no ] && log_end_msg 0
                    ;;
                2) [ "$VERBOSE" != no ] && log_end_msg 1
                    ;;
            esac
            ;;
        status)
            status_of_proc $NAME $NAME && exit 0 || exit $?
            ;;
        #reload|force-reload)
            # If do_reload() is not implemented then leave this commented out
            # and leave 'force-reload' as an alias for 'restart'.

            #log_daemon_msg "Reloading $DESC" "$NAME"
            #do_reload
            #log_end_msg $?
            #;;
        restart|force-reload)
            # If the "reload" option is implemented then remove the
            # 'force-reload' alias

            log_daemon_msg "Restarting $DESC" "$NAME"
            do_stop
            sleep 1
            case "$?" in
                0|1)
                    do_start
                    case "$?" in
                        0) log_end_msg 0 ;;
                        1) log_end_msg 1 ;; # Old process is still running
                        *) log_end_msg 1 ;; # Failed to start
                    esac
                    ;;
                *)
                    # Failed to stop
                    log_end_msg 1
                    ;;
            esac
            ;;
        *)
            #echo "Usage: $SCRIPTNAME {start|stop|restart|reload|force-reload}" >&2
            echo "Usage: $SCRIPTNAME {start|stop|status|restart|force-reload}" >&2
            exit 3
            ;;
    esac
fi
