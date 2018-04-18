#!/usr/bin/tcsh
cd /home/wayadmin/scripts/alerts/wayAlerts
echo `date` "Running wayAlerts " >> wayAlerts.log
python ./wayAlerts.py
