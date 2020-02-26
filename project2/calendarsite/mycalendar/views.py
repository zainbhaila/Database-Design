from django.shortcuts import render, get_object_or_404

from django.http import HttpResponse, HttpResponseRedirect
from mycalendar.models import User, Calendar, Event, BelongsTo
from django.urls import reverse
import datetime
from django.utils import timezone


# Create your views here.

def mainindex(request):
        context = { 'user_list': User.objects.all() }
        return render(request, 'mycalendar/index.html', context)

def userindex(request, user_id):
        context = { 'calendar_list': User.objects.get(pk=user_id).calendar_set }
        return render(request, 'mycalendar/userindex.html', context)

def eventindex(request, event_id):
        event = Event.objects.get(pk=event_id)
        statuses = [(c.title, BelongsTo.Status(BelongsTo.objects.get(event=event, calendar=c).status)) for c in event.calendars.all()]
        context = {'event': event, 'statuses': statuses}
        return render(request, 'mycalendar/eventindex.html', context)

def calendarindex(request, calendar_id):
        elist = Calendar.objects.get(pk=calendar_id).event_set.all().order_by('start_time')
        elist2 = []
        last = None
        count = 0
        elist2.append([])
        for i in elist: # break up into seperate dates
            if last == None:
                last = i.start_time.date()
            if last != i.start_time.date():
                count += 1
                elist2.append([])
                last = i.start_time.date()
            elist2[count].append(i)
        context = { 'calendar_id': calendar_id, 'event_list': elist2 }
        return render(request, 'mycalendar/calendarindex.html', context)

def createevent(request, user_id):
        context = { 'user': User.objects.get(pk=user_id), 'calendar_list': Calendar.objects.all() }
        return render(request, 'mycalendar/createevent.html', context)

def submitcreateevent(request, user_id):
        chosen_calendars = [c for c in Calendar.objects.all() if request.POST["answer{}".format(c.id)] == "true"]
        e = Event(title=request.POST["title"], start_time=request.POST["start_time"], end_time=request.POST["end_time"], created_by = User.objects.get(pk=user_id))
        e.save()
        for c in chosen_calendars:
            bt = BelongsTo(event=e, calendar=c, status=BelongsTo.Status.WAITING_RESPONSE)
            bt.save()
        return HttpResponseRedirect(reverse('createdevent', args=(user_id,e.id,)))

def createdevent(request, user_id, event_id):
    return eventindex(request, event_id)

def modifyevent(request, event_id):
        event = Event.objects.get(pk=event_id)
        event.start_format = event.start_time.strftime('%Y-%m-%d %H:%M') # reformat dates to YYYY-MM-DD HH:MM
        event.end_format = event.end_time.strftime('%Y-%m-%d %H:%M')
        context = {'calendar_list': Calendar.objects.all(), 'event': event }
        return render(request, 'mycalendar/modifyevent.html', context)

def submitmodifyevent(request, event_id):
        event = Event.objects.get(pk=event_id)
        chosen_calendars = [c for c in Calendar.objects.all() if request.POST["answer{}".format(c.id)] == "true"]
        all_calendars = Calendar.objects.all()
        e = Event(id=event_id, title=request.POST["title"], start_time=request.POST["start_time"], end_time=request.POST["end_time"], created_by = event.created_by)
        e.save()
        for c in all_calendars: # remove old event from all calendars
            BelongsTo.objects.filter(event=event, calendar=c).delete()
        for c in chosen_calendars: # post new status to all calendars
            bt = BelongsTo(event=e, calendar=c, status=BelongsTo.Status.WAITING_RESPONSE)
            bt.save()
        return HttpResponseRedirect(reverse('modifiedevent', args=(e.id,)))

def modifiedevent(request, event_id):
    return eventindex(request, event_id)

def waiting(request, user_id, calendar_id):
        print(calendar_id)
        cal = Calendar.objects.get(pk=calendar_id)
        events = cal.event_set.all()
        waiting = [w for w in events if BelongsTo.Status(BelongsTo.objects.get(event=w, calendar=cal).status) == "WR"]
        context = {'waiting': waiting, 'user_id': user_id, 'calendar_id': calendar_id}
        return render(request, 'mycalendar/waiting.html', context)

def submitwaiting(request, user_id, calendar_id):
        cal = Calendar.objects.get(pk=calendar_id)
        events = cal.event_set.all()
        waiting = [w for w in events if BelongsTo.Status(BelongsTo.objects.get(event=w, calendar=cal).status) == "WR"]
        for w in waiting:
            BelongsTo.objects.filter(event=w, calendar=cal).delete()
        for w in waiting:
            resp = request.POST["r{}".format(w.id)]
            match = BelongsTo.Status.WAITING_RESPONSE
            if resp == "AC":
                match = BelongsTo.Status.ACCEPTED
            elif resp == "DE":
                match =  BelongsTo.Status.DECLINED
            elif resp == "TE":
                match = BelongsTo.TENTATIVE
            bt = BelongsTo(event=w, calendar=cal, status=match)
            bt.save()
        context = {'waiting': waiting}
        return HttpResponseRedirect(reverse('submitedwaiting', args=(calendar_id,)))

def submitedwaiting(request, calendar_id):
    return calendarindex(request, calendar_id)

# Here we will compute some statistics about the data in the database
# Specifically: for each calendar, we will compute the number of events in different status, and a total as a 6-tuple
# The tuple fields are: (calendar title, number of waiting response events, number of accepted events, number of declined events, number of tentative events, total)
# ('John\'s Work Calendar, 1, 4, 3, 4, 10)
def summary(request):
    calendars = Calendar.objects.all()
    summary_tuples = []
    for i in calendars:
        events = Calendar.objects.get(pk=i.id).event_set.all()
        total = 0
        wr = 0
        ac = 0
        de = 0
        te = 0
        for j in events: # event response counting
            total += 1
            status = BelongsTo.Status(BelongsTo.objects.get(event=j, calendar=i).status)
            if status == "WR":
                wr += 1
            elif status == "AC":
                ac += 1
            elif status == "DE":
                de += 1
            elif status == "TE":
                te += 1
        summary_tuples.append((i.title, wr, ac, de, te, total))
    context = {'summary_tuples': summary_tuples}
    return render(request, 'mycalendar/summary.html', context)
