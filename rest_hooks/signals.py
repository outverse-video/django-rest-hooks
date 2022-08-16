import django
from django.dispatch import Signal


if django.VERSION >= (3, 0):
    hook_event = Signal()
    raw_hook_event = Signal()
    hook_sent_event = Signal()
else:
    hook_event = Signal(providing_args=['action', 'instance'])
    raw_hook_event = Signal(providing_args=['event_name', 'payload', 'user'])
    hook_sent_event = Signal(providing_args=['payload', 'instance', 'hook'])
