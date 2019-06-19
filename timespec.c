#include <time.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
extern char *strptime();

static char fmt1[] = "%FT%T";
static char fmt2[] = "%FT%T%z";

char *
convert_time(char *p, struct timespec *ts, char *errbuf)
{
	struct tm tb[1];
	char *q, *xx;
	char *x = 0;
	int fr = 0;
	int c = 9;
	char *fmt;

	if ((q = strchr(p, '.'))) {
		x = strdup(p);
		memcpy(x, p, q-p);
		xx = x+(q-p);
		*xx = 0;
		c = 0;
		while (*++q) {
			if (*q < '0' || *q > '9') break;
			fr *= 10;
			fr += *q-'0';
			++c;
		}
		fmt = fmt1;
		if (*q != 'Z') {
			strcpy(xx, q);
			fmt = fmt2;
		}
		p = x;
	}
	memset(ts, 0, sizeof *ts);
	memset(tb, 0, sizeof *tb);
	q = strptime(p, fmt, tb);
	if (!q) {
		sprintf (errbuf,"got NULL from strptime on %s", p);
		if (x) free(x);
		return errbuf;
	}
	if (*q) {
		sprintf(errbuf,"failed at offset %d: %s", q-p, q);
		if (x) free(x);
		return errbuf;
	}
	ts->tv_sec = timegm(tb);
	while (c < 9) {
		++c;
		fr *= 10;
	}
	while (c > 9) {
		--c;
		fr /= 10;
	}
	ts->tv_nsec = fr;
	return 0;
}

char *
fix_format_microseconds(char *ts)
{
	char *cp = strchr(ts, '.');
	int c;
	if (cp && strlen(cp) > 7) {
		c = cp[7];
		cp[7] = 0;
		if (c >= '5') {
			int us = strtoll(cp+1, 0, 0);
			++us;
			snprintf(cp+1, 7, "%06d", us);
		}
	}
	return ts;
}

char *
format_timespec(char *buf, int n, struct timespec *tv)
{
	snprintf(buf, n, "%ld.%09ld", tv->tv_sec, tv->tv_nsec);
	return buf;
}

double timespec_to_float(struct timespec *tv)
{
	double r;
	r = tv->tv_nsec;
	r /= 1000000000;
	r += tv->tv_sec;
	return r;
}

void
timespec_diff(struct timespec *st, struct timespec *en)
{
	if (en->tv_nsec < st->tv_nsec) {
		en->tv_sec -= 1;
		en->tv_nsec += 1000000000;
	}
	en->tv_sec -= st->tv_sec;
	en->tv_nsec -= st->tv_nsec;
}

void
timespec_add(struct timespec *from, struct timespec *ac)
{
	ac->tv_sec += from->tv_sec;
	ac->tv_nsec += from->tv_nsec;
	if (ac->tv_nsec >= 1000000000) {
		ac->tv_sec += 1;
		ac->tv_nsec -= 1000000000;
	}
}
