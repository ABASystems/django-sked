import copy
import heapq
from datetime import datetime, timedelta, time
import itertools

from dateutil.rrule import rrulestr
from django.utils import timezone

# from .models import RepeatingEvent, Event, Accrual


class UnboundedOverlapError(Exception):
    def __init__(self):
        super().__init__('unbounded overlap detected')


class AggregationError(Exception):
    pass


def make_future_dtrange(dtrange):
    """ Convert a time range to be lower bounded from now.
    """
    now = timezone.now().date() + timedelta(1)
    if dtrange[0] is not None and dtrange[0] >= now:
        return dtrange
    elif dtrange[1] is None or dtrange[1] > now:
        return (now, dtrange[1])
    else:
        return (now, now)


def make_repeating_event_iterator(rev, future):
    cur_range = (
        max(future[0], rev.range.lower),
        min(future[1], rev.range.upper) if rev.range.upper is not None else future[1]
    )
    rr = rrulestr(rev.repetition, dtstart=cur_range[0])
    for dt in rr.xafter(datetime.combine(cur_range[0], time()), inc=True):
        date = dt.date()
        if date >= cur_range[1]:
            raise StopIteration
        yield (date, rev)


def canonicalize_events(events):
    events = events if events is not None else []
    events = [(e.source, e.occurred)
              for e in events
              if events.parent is None and events.source]
    return set(events)


def overlapping_repeating_events(RepeatingEvent, dtrange):
    """ Yield all repeating events that overlap the provided time range.
    """
    future = make_future_dtrange(dtrange)
    if future[1] is None:
        raise UnboundedOverlapError
    all_revs = []
    for rev in RepeatingEvent.objects.filter(range__overlap=future):  # TODO: chunking
        all_revs.append(iter(make_repeating_event_iterator(rev, future)))
    if len(all_revs) > 1000:
        raise AggregationError('probably too many repeating-event objects')
    for date, rev in heapq.merge(*all_revs, key=lambda x: x[0]):
        yield rev.instantiate(date)


def overlapping_events(Event, dtrange):
    """ Yield all non-repeating events that overlap the provided time range.

    Note that this will return events that are amendments. This is because I
    want the sources to be available for the `overlap` function in order to
    remove future duplicates. I will filter out non-amendments in `overlap`.
    """
    query = {}
    if dtrange[0] is not None:
        query['occurred__gte'] = dtrange[0]
    if dtrange[1] is not None:
        query['occurred__lt'] = dtrange[1]
    ev_qs = Event.objects.filter(**query).order_by('occurred')  # TODO: chunking
    for ev in ev_qs:
        yield ev


def overlap(Event, RepeatingEvent, dtrange):
    """ Find all overlapping events.
    """
    all_evs = [
        iter(overlapping_events(Event, dtrange)),
        iter(overlapping_repeating_events(RepeatingEvent, dtrange))
    ]
    lce_date = None  # last concrete event date
    lce_sources = set()  # last concrete event sources
    for ev in heapq.merge(*all_evs, key=lambda x: x.occurred):

        # This part allows us to prevent duplicate generation of future events
        # when they have been pregenerated. It hinges on `heapq.merge` respecting
        # iterator order; I pass in the concrete event iterator first.
        if ev.id is not None:
            if lce_date != ev.occurred:
                lce_date = ev.occurred
                lce_sources = set()
            if ev.source:
                lce_sources.add(ev.source.id)
        elif lce_date is not None and lce_date == ev.occurred and ev.source.id in lce_sources:
            continue

        # Don't send through concrete events that have amendments.
        if ev.amendments.exists():  # TODO: perhaps use a boolean "is_amended" for performance?
            continue

        yield ev


def aggregate(Event, RepeatingEvent, dtrange, op, initial=None,
              exclude_tagged=True):
    """ Perform an aggregate over overlapping events.

    By default any event with tags is excluded from the aggregation. To include
    tagged events set `exclude_tagged` to True.
    """
    val = initial
    for ev in overlap(Event, RepeatingEvent, dtrange):
        if exclude_tagged and len(ev.tags.keys()):
            continue
        val = op(val, op.coerce(ev))
    return op.final(val)


def aggregate_tags(Event, RepeatingEvent, dtrange, op, initial={}):
    """ Aggregate event tags.
    """
    val = copy.deepcopy(initial)
    for ev in overlap(Event, RepeatingEvent, dtrange):
        for kk in val.keys():
            if not ev[1].tags or kk in ev[1].tags:
                val[kk] = op(val[kk], op.coerce(ev))
    for kk, vv in val.items():
        val[kk] = op.final(vv)
    return val


# def accrue(dt, op, initial_tags=[]):
#     """ Aggregate up until a datetime.
#     """
#     now = timezone.now().date()
#     latest = Accrual.objects.filter(timestamp__lte=now).order_by('-timestamp')
#     if latest:
#         base = reduce(
#             op, [latest[0].values.get(t, 0) for t in initial_tags], None
#         )
#         start = latest[0].timestamp
#     else:
#         base = None
#         start = None
#     addition = aggregate((start, dt), op)  # TODO: initial?
#     return op(base, addition)
