# treadmill cron
Vary your walking speed and incline on your treadmill desk.

Only works with the nordictrack 6.5s treadmill at the moment. If you can code *a little* you may be able to adapt it ot your treadmill.

AI-generated an unreviewed code.

## Motivation
Treadmill desks are great. You often want to just walk while your treadmill runs rather than messing with settings.  I plot along at 1.4 -2.0 kph for hours at an end.  Treadmill cron allows you to automate some intervals or variation through your day to get you some free exercise. 

Intense exercise has certain health benefits including hormonal effects which reduce visceral fat so make a good addition to low intensity exercise.

## Features
* Increase speed at certain times during the day or every hour
* Have the speed "creep up" for other times of the day. This is useful if you get tired.

## Installatation
pipx install nord-ich-track
pipx install treadmill-cron

## Usage
Start nord-ich-track in daemon mode. 

Generate a schedule file then run:

```
:00-:05  3.0  5.0
```

This sets the speed to 3.0 and incline 5 for five minutes every hour.


`treadmill-cron schedule`


## Making this work on another treadmill
I wrote nord-ich-track mostly with an LLM by giving it access to the open source qzdomyos project and also recording bluetooth cpatures from qzdomyos. I did this becasue I could not easily get qzdomyoos to build in a way that I could control it remotely.

If your treadmill is supported by the wonderful qzdomyos you can likely do the same. qzdomyoos is wonderful, but is written in C++ and uses qt, which creates certain problems.

## LLM use
I use an LLM to generate my config file. If you give it access to this source code it can likely do things for you.

