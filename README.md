# treadmill cron
Vary walking speed and incline on your treadmill desk (r/musicalTreadmillDesk)

Also provides interactive features rout routines.

This only works with the nordictrack 6.5s treadmill at the moment via nord-ich-track. If you can code *a little* you may be able to adapt treadmill-cron to your treadmill.

AI-generated an unreviewed code.

## Motivation
Treadmill desks are great. You often want to just walk while your treadmill runs rather than messing with settings.  I plod along at 1.4 -2.0 kph for hours at an end.  Treadmill cron allows you to automate some intervals or variation through your day to get you some free exercise. 

Intense exercise has certain health benefits including hormonal effects which reduce visceral fat so make a good addition to low intensity exercise.

## Features
* Increase speed at certain times during the day or every hour
* Have different types of day (such as an endurance, speed and recovery days)
* Have the speed "creep up" for other times of the day. This is useful if you get tired.
* Interactively run a routine when you want it in response to a button being pressed (see "speed play")

## Installatation

```
pipx install nord-ich-track
pipx install treadmill-cron
```

## Usage
Start nord-ich-track in daemon mode with `nord-ick-track daemon`

Create a schedule file, `treadmill.schedule`. This sets the speed to 3.0 and incline 5 for five minutes every hour.

```
:00-:05  3.0  5.0
```

You can then run, `treadmill-cron treadmill.schedule` to run this rountine.

To overwrite this setting you can use `treadmill-cron now`.

treadmill-cron supports modes. Entries labelled with a mode only run when in this model.

`treadmill-cron events` output events when blocks start and stop. You can use this to trigger actions such as stopping or playiong music or starting a fan.


## Making this work on another treadmill
I wrote nord-ich-track mostly with an LLM by giving it access to the open source qzdomyos project and also recording bluetooth cpatures from qzdomyos. I did this becasue I could not easily get qzdomyoos to build in a way that I could control it remotely.

If your treadmill is supported by the wonderful qzdomyos you can likely do the same. qzdomyoos is wonderful, but is written in C++ and uses qt, which creates certain problems.

## LLM use
I use an LLM to generate my config file. If you give youe `llm` access to this source code it can likely do things for you.

