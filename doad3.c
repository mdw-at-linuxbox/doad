#include <stdio.h>
#include <stdlib.h>
#include <curl/curl.h>

char *my_method = "GET";
char *my_user;
char *my_password;
char *my_url;
char *my_default_url = "https://acanthodes.eng.arb.redhat.com:35357/v3";
int multi_count;
char *capath;
int Vflag;

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

uint
my_receive_http_data(char *in, uint size, uint num, void *h)
{
	uint r;
	r = size * num;
#if 0
	fprintf(stdout,"Received: ");
	r = fwrite(in, 1, r, stdout);
#endif
	return r;
}

struct curl_carrier {
	int uses;
	CURL *h;
};

#define NC 30
struct curl_carrier *list[NC];
int list_start, list_end;
pthread_mutex_t saved_mutex;

struct curl_carrier *
get_curl_handle()
{
	struct curl_carrier *ca = 0;
	CURL *h;
        pthread_mutex_lock(&saved_mutex);
	if (list_start != list_end) {
		ca = list[list_start];
		++list_start;
		if (list_start >= NC)
			list_start = 0;
	}
	pthread_mutex_unlock(&saved_mutex);
	if (ca) {
	} else if ((h = curl_easy_init())) {
		++carrier_allocs;
		ca = malloc(sizeof *ca);
		if (!ca) {
			curl_easy_cleanup(h);
			return 0;
		}
		ca->h = h;
		ca->uses = 0;
		++easy_inits_made;
	} else {
		// ca = 0;
	}
	return ca;
}

void
release_curl_handle_now(struct curl_carrier *ca)
{
	curl_easy_cleanup(ca->h);
	free(ca);
}

void
release_curl_handle_now(struct curl_carrier *ca)
{
	int i;
	if (++ca->uses > multi_count) {
		release_curl_handle_now(ca);
		return;
	}
        pthread_mutex_lock(&saved_mutex);
	i = list_end;
	++i;
	if (i >= NC) i = 0;
	if (i != list_start) {
		list[list_end] = ca;
		list_end = i;
		curl_easy_reset(ca->h);
		ca = 0;
	}
	pthread_mutex_unlock(&saved_mutex);
	if (ca) release_curl_handle_now(ca);
}

int cleaner_shutdown;
pthread_t cleaner_id;
pthread_cond_t cleaner_cond;
#define MAXIDLE 5

void *curl_cleaner(void *h)
{
	struct curl_carrier *ca;
	struct timespec now[1];

	if (pthread_mutex_lock(&saved_mutex) < 0) {
		fprintf(stderr,"lock failed %d\n", errno);
	}
	for (;;) {
		timespec until[1];
		if (cleaner_shutdown) {
			if (list_start == list_end)
				break;
		} else {
			if (clock_gettime(CLOCK_REALTIME, now) < 0) {
				fprintf(stderr,"gettime failed %d\n", errno);
			}
			now->tv_sec += MAXIDLE;
			if (pthread_cond_timedwait(&cleaner_cond, &saved_mutex, now) < 0) {
				fprintf(stderr,"cond wait failed %d\n", errno);
			}
		}
		if (clock_gettime(CLOCK_MONOTONIC_RAW, now) < 0) {
			fprintf(stderr,"gettime failed %d\n", errno);
		}
		while (list_start != list_end) {
			int i;
			i = list_end;
			--i;
			if (i < 0) i = NC-1;
			ca = list[i];
			if (!cleaner_shutdown && ca->lastuse->tv_sec
					>= now->tv_sec + MAXIDLE)
				break;
			list_end = i;
			release_curl_handle_now(ca);
		}
	}
	if (pthread_mutex_unlock(&saved_mutex) < 0) {
		fprintf(stderr,"unlock failed %d\n", errno);
	}
}

void
init_curl_handles()
{
	pthread_create(&cleaner_id, NULL, curl_cleaner, nullptr);
}

void
flush_curl_handles()
{
	struct curl_carrier *curl;
	void *result;

	cleaner_shutdown = 1;
	pthread_cond_signal(&cleaner_cond);
	if (pthread_join(cleaner_id, &result) < 0) {
		fprintf(stderr,"pthread_join failed %d\n", errno);
		return;
	}
	if (list_start != list_end) {
		std::cerr << "cleaner failed final cleanup" << std::endl;
	}
}

get_token()
{
	struct curl_carrier *ca;
	CURL *curl;
	CURLcode rc;
	char error_buf[CURL_ERROR_SIZE];
	long http_status;
	int r = 0;
	struct receiver_arg recvarg[1];

	ca = get_curl_handle();
	if (!ca) return 1;
	curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, my_method);
	curl_easy_setopt(curl, CURLOPT_URL, my_url);
	curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
	curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
	curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, error_buf);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, my_receive_http_data);
	if (Vflag) {
		curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
	}
	if (capath)
		curl_easy_setopt(curl, CURLOPT_CAINFO, capath);
	rc = curl_easy_perform(curl);
	if (rc != CURLE_OK) {
		fprintf(stderr,"curl_easy_perform failed, %s\n",
			curl_easy_strerror(rc));
		r |= 2;
		goto Done;
	}
	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_status);
Done:
	if (--save_until < 1) {
		curl_easy_cleanup(curl);
		saved_curl = 0;
	} else {
		saved_curl = curl;
	}
	return r;
}

struct work_element {
	char *op;
	char *work;
	struct work_element *next;
};

struct work_element *work_queue;

int
read_in_data()
{
	char line[512];
	char *cp;
	int lineno;
	char *ep, *s, *q;
	char *op;
	char *what;
	struct work_element *wp;
	struct work_element **wpp;
	lineno = 0;
	wpp = &work_queue;
	while (fgets(line, sizeof line, stdin)) {
		++lineno;
		cp = strchr(line, '\n');
		if (cp) *cp = 0;
		what = op = NULL;
		for (s = line;
			q = strtok_r(s, " \t", &ep;
			s = NULL) {
			if (!op) op = q;
			else if (!what) what = q;
			else {
				fprintf (stderr, "Extra data not understood: <%s>\n",
					q);
				return 1;
			}
		}
		if (!op) {
			fprintf (stderr,"Missing op at line %d\n", lineno);
			return 1;
		}
		if (!what) {
			fprintf (stderr,"Missing filename at line %d\n", lineno);
			return 1;
		}
		cp = malloc(sizeof *wp + 2 + strlen(op) + strlen(what));
		wp = (struct work_element *) cp;
		cp += sizeof *wp;
		memset(wp, 0, sizeof*wp);
		wp->op = cp;
		i = strlen(op) + 1;
		memcpy(cp, op, i);
		cp += i;
		wp->what = what;
		strcpy(cp, what);
		*wpp = wp;
		wpp = &wp->next;
	}
	return 0;
}

void
start_threads()
{
}

void
wait_for_completion()
{
}

void
report_results()
{
}

int process()
{
	int r;
	if (r = read_in_data()) {
		fprintf(stderr,"read_in_data failed\n");
		return r;
	}
	if (r = get_token()) {
		fprintf(stderr,"get_token failed\n");
		return r;
	}
	start_threads();
	wait_for_completion();
	report_results();
	return r;
}

int main(int ac, char **av)
{
	char *ap, *ep, *cp;
	char *msg;
	int r;
	char *change_here;

	while (--ac > 0) if (*(ap = *++av) == '-') while (*++ap) switch(*ap) {
//	case 'v':
//		++vflag;
//		break;
	case '-':
		break;
	case 'C':
		if (ac < 1) {
			goto Usage;
		}
		--ac;
		capath = *++av;
		break;
	case 'V':
		++Vflag;
		break;
	case 't':
		if (ac < 1) {
			goto Usage;
		}
		--ac;
		my_token = *++av;
		break;
	case 'c':
		if (ac < 1) {
			goto Usage;
		}
		--ac;
		change_here = *++av;
		break;
	case 'u':
		if (ac < 1) {
			goto Usage;
		}
		--ac;
		my_user = *++av;
		break;
	case 'p':
		if (ac < 1) {
			goto Usage;
		}
		--ac;
		my_password = *++av;
		break;
	case 'h':
		if (ac < 1) {
			goto Usage;
		}
		--ac;
		my_url = *++av;
		break;
	case 'm':
		if (ac < 1) {
			goto Usage;
		}
		--ac;
		cp = *++av;
		multi_count = strtoll(cp, &ep, 0);
		if (cp == ep || *ep) {
			fprintf(stderr,"Bad multicount <%s>\n", cp);
			goto Usage;
		}
		break;
	default:
	Usage:
		fprintf(stderr,"Usage: doad3 [-u x] [-p x] [-t x] [-h hosturl] [-C capath] [-V]\n");
		exit(1);
	} else if (!my_url) {
		my_url = ap;
	} else {
		fprintf(stderr,"extra arg?\n");
		goto Usage;
	}
	if (!change_here)
		;
	else if (chdir(change_here) < 0) {
		fprintf(stderr,"Error doing chdir to %s\n", change_here);
		exit(1);
	}
	if (!my_url)
		my_url = getenv("MY_URL");
	if (!my_url)
		my_url = my_default_url;
	curl_global_init(CURL_GLOBAL_DEFAULT);
	init_curl_handles();
	r = process();
	flush_curl_handles();
	curl_global_cleanup();
	exit(r);
}
