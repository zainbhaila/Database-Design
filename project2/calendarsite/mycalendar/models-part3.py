from django.db import models
from django.utils.translation import gettext_lazy as _
from django.core.validators import MaxValueValidator, MinValueValidator, RegexValidator

# Create your models here.
import datetime
from django.utils import timezone

class User(models.Model):
        name = models.CharField(max_length=50)
        employs = models.ForeignKey(Company, on_delete=models.CASCADE) # employs relationship
        def __str__(self):
                return self.name

class Calendar(models.Model):
        title = models.CharField(max_length=50)
        owner = models.ForeignKey(User, on_delete=models.CASCADE)
        description = models.CharField(max_length=500)
        visibility = models.ManyToManyField(User, through='VisibleTo') # visibility many to many relationship
        def __str__(self):
                return self.title

class Event(models.Model):
        title = models.CharField(max_length=50)
        start_time = models.DateTimeField()
        end_time = models.DateTimeField()
        calendars = models.ManyToManyField(Calendar, through='BelongsTo')
        created_by = models.ForeignKey(User, on_delete=models.CASCADE)
        scheduled_in = models.ForeignKey(Room, on_delete=models.CASCADE) # foreign key to link to room
        def __str__(self):
                return self.title

class Recurrence(models.Model): # used to track repeating events
    is_recurring = models.BooleanField(default=False) # is an event a recurring event
    type = models.IntegerField(default=0, validators=[MaxValueValidator(5), MinValueValidator(0)]) # 0 - nonrepeating, 1 - daily, 2 - weekly, 3 - monthly, 4 - yearly, 5 - days of the week
    days_of_week = models.CharField(max_length=7, validators=[RegexValidator(r'^[1-7]*$', 'Only numbers 1-7 are allowed.')], blank=True, null=True) # string containing days of the week if type is 5, 1 is Sunday, 7 is Saturday
    end_date = models.DateTimeField() # when the recurrence ends
    scheduled_by = models.ForeignKey(Event, on_delete=models.CASCADE) # link the recurrence to a specific event
    def __str__(self):
            return "{} is type {}".format(self.scheduled_by, self.type)

class Company(models.Model): # company model has name and address
    name = models.CharField(max_length=50)
    address = models.CharField(max_length=500)
    def __str__(self):
            return self.name

class Room(models.Model): # room model has number, name, and capacity
    number = models.CharField(max_length=50)
    name = models.CharField(max_length=50, null=True, blank=True)
    capacity = models.IntegerField(default=20, validators=[MaxValueValidator(1000), MinValueValidator(0)])
    def __str__(self):
            return self.name

class VisibleTo(models.Model): # allow users to set who can see what
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    calendar = models.ForeignKey(Calendar, on_delete=models.CASCAD)E)
    visible = models.BooleanField(default=False)
    def __str__(self):
            return "{} sees {}: {}".format(self.user, self.calendar, self.visible)

class BelongsTo(models.Model):
    class Status(models.TextChoices):
        ACCEPTED = 'AC', _('Accepted')
        DECLINED = 'DE', _('Declined')
        TENTATIVE = 'TE', _('Tentative')
        WAITING_RESPONSE = 'WR', _('Waiting Response')

    event = models.ForeignKey(Event, on_delete=models.CASCADE)
    calendar = models.ForeignKey(Calendar, on_delete=models.CASCADE)
    status = models.CharField(max_length=2, choices=Status.choices, default=Status.WAITING_RESPONSE)
    priority = models.IntegerField(default=5, validators=[MaxValueValidator(5), MinValueValidator(1)]) # priority for each event in a calendar
    def __str__(self):
            return "{} in {}: {}".format(self.event, self.calendar, self.status)
