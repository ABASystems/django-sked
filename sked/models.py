from django.db import models
from django.contrib.postgres.fields import DateRangeField, JSONField


# class EventMetaclass(ModelBase):
#     def __new__(mcs, name, bases, attrs):
#         new_class = super(EventMetaclass, mcs).__new__(mcs, name, bases, attrs)
#         meta = new_class._meta
#         re_cls = meta.repeating_event_class


class Event(models.Model):
    """ A concrete event.

    The `amended` value allows for a hierarchy of changes/groups. This can
    be used to track actual values vs anticipated values.
    """
    amended = models.ForeignKey('self', related_name='amendments',
                                blank=True, null=True)
    created = models.DateTimeField(auto_now_add=True)
    occurred = models.DateField()
    tags = JSONField(default={}, blank=True)

    class Meta:
        abstract = True

    def __str__(self):
        return '{}'.format(self.occurred)


class RepeatingEvent(models.Model):
    """ An event that occurs multiple times with regular intervals.
    """
    repetition = models.CharField(max_length=50)
    range = DateRangeField(blank=True)
    tags = JSONField(default={}, blank=True)
    factory = JSONField(default={}, blank=True)

    class Meta:
        abstract = True

    def __str__(self):
        # TODO: Get human readable form.
        return self.repetition

    def instantiate(self, dt, commit=False):
        ev = self.make_event(dt)
        ev.occurred = dt
        ev.tags = self.tags
        ev.source = self
        if commit:
            ev.save()
        return ev


class Accrual(models.Model):
    """ An accrual up until a point in time.
    """
    timestamp = models.DateField()
    values = JSONField(default={})
