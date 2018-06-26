char * convert_time(char *p, struct timespec *ts, char *errbuf);
char * fix_format_microseconds(char *ts);
char * format_timespec(char *buf, int n, struct timespec *tv);

double timespec_to_float(struct timespec *tv);

void timespec_diff(struct timespec *st, struct timespec *en);
void timespec_add(struct timespec *from, struct timespec *ac);
