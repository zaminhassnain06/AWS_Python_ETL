"""Common Functions Used In Project"""
from datetime import datetime


def current_date_time():
    """datetime object containing current date and time"""
    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
    return dt_string
