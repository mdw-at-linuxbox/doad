#include <stdio.h>
#include <stdlib.h>
#include <curl/curl.h>
#include <pthread.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <json-c/json.h>

char *my_method = "GET";
char *my_user;
char *my_password;
char *my_url;
int multi_count;
char *capath;
char *my_token;
char *my_tenant;
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

struct curl_carrier {
	int uses;
	struct timespec lastuse[1];
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
		ca = malloc(sizeof *ca);
		if (!ca) {
			curl_easy_cleanup(h);
			return 0;
		}
		ca->h = h;
		ca->uses = 0;
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
release_curl_handle(struct curl_carrier *ca)
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
		struct timespec until[1];
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
	pthread_create(&cleaner_id, NULL, curl_cleaner, NULL);
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
		fprintf(stderr,"cleaner failed final cleanup\n");
	}
}

struct token_arg {
	struct json_tokener *json_tokeniser;
};

uint
eat_keystone_data(char *in, uint size, uint num, void *h)
{
	uint r;
	r = size * num;
	struct token_arg *a = h;
	json_object *jobj;
	enum json_tokener_error je;
#if 0
	fprintf(stdout,"Received: ");
	r = fwrite(in, 1, r, stdout);
#endif
	jobj = json_tokener_parse_ex(a->json_tokeniser, in, r);
	je = json_tokener_get_error(a->json_tokeniser);
	if (je != json_tokener_success) {
		fprintf(stderr,"Cannot parse json: e=%d string=<%.*s>\n",
			r, in);
	} else {
		const char *response_json = json_object_to_json_string_ext(jobj, JSON_C_TO_STRING_PRETTY);
// if (Vflag)
printf ("RECV: <%s>\n", response_json);
	}
	return r;
}

int
get_token()
{
	struct curl_carrier *ca;
	CURL *curl;
	struct curl_slist *headers = 0;
	CURLcode rc;
	char error_buf[CURL_ERROR_SIZE];
	long http_status;
	char *temp_url = 0;
	int i;
	int r = 0;
	struct token_arg recvarg[1];
	json_object *jx = json_object_new_object();
	json_object *jpu = json_object_new_object();
	json_object *jt = json_object_new_object();
	json_object *jm = json_object_new_array();
	json_object *jy = json_object_new_object();
	json_object *jz = json_object_new_object();
	json_object *jobj = json_object_new_object();

	temp_url = malloc(i = strlen(my_url) + 30);
	snprintf(temp_url, i, "%s/auth/tokens", my_url);
	json_object_array_add(jm, json_object_new_string("password"));
	json_object_object_add(jx, "name", json_object_new_string("Default"));
	json_object_object_add(jpu, "domain", jx);
	json_object_object_add(jpu, "name", json_object_new_string(my_user));
	json_object_object_add(jpu, "password", json_object_new_string(my_password));
	json_object_object_add(jy, "user", jpu);
	json_object_object_add(jt, "password", jy);
	json_object_object_add(jt, "methods", jm);
	json_object_object_add(jz, "identity", jt);
	json_object_object_add(jobj, "auth", jz);

	const char *req_json = json_object_to_json_string_ext(jobj, JSON_C_TO_STRING_PLAIN);
	recvarg->json_tokeniser = json_tokener_new();
if (Vflag) printf ("SEND: <%s>\n", req_json);
	headers = curl_slist_append(headers, "Content-Type: " "application/json");
	headers = curl_slist_append(headers, "Accept: " "application/json");
	headers = curl_slist_append(headers, "Expect:");
	ca = get_curl_handle();
	if (!ca) return 1;
	curl = ca->h;
//	curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, my_method);
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
	curl_easy_setopt(curl, CURLOPT_URL, temp_url);
	curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
	curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
	curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, error_buf);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*)recvarg);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, eat_keystone_data);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, req_json);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, strlen(req_json));
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
	release_curl_handle(ca);
	curl_slist_free_all(headers);
//	free(req_json);
	if (temp_url) free(temp_url);
	return r;
}

#define W_ADD 1
#define W_DEL 2
struct work_element {
	int op;
	char *what;
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
	int i;
	int r = 0;

	lineno = 0;
	wpp = &work_queue;
	while (fgets(line, sizeof line, stdin)) {
		++lineno;
		cp = strchr(line, '\n');
		if (cp) *cp = 0;
		what = op = NULL;
		for (s = line;
			(q = strtok_r(s, " \t", &ep))!= NULL;
			s = 0) {
			if (!op) op = q;
			else if (!what) what = q;
			else {
				fprintf (stderr, "Extra data not understood: <%s>\n",
					q);
				return 1;
			}
		}
		if (!op) {
			fprintf (stderr,"Missing op at data line %d\n", lineno);
			return 1;
		}
		if (!strcmp(op, "ADD"))
			i = W_ADD;
		else if (!strcmp(op, "DEL"))
			i = W_DEL;
		else {
			fprintf (stderr,"Bad op <%s> at data line %d\n", op, lineno);
			r = 1;
			continue;
		}
		if (!what) {
			fprintf (stderr,"Missing arg at data line %d\n", lineno);
			r = 1;
			continue;
		}
		cp = malloc(sizeof *wp + 1 + strlen(what));
		wp = (struct work_element *) cp;
		cp += sizeof *wp;
		memset(wp, 0, sizeof*wp);
		wp->op = i;
		wp->what = what;
		strcpy(cp, what);
		*wpp = wp;
		wpp = &wp->next;
	}
	return r;
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
	if (my_token) {
	} else if (r = get_token()) {
		fprintf(stderr,"get_token failed\n");
		return r;
	}
	start_threads();
	wait_for_completion();
	report_results();
	return r;
}

int
main(int ac, char **av)
{
	char *ap, *ep, *cp;
	char *msg;
	int r;
	char *change_here = 0;

	if (!my_user)
		my_user = getenv("OS_USERNAME");
	if (!my_password)
		my_password = getenv("OS_PASSWORD");
	if (!my_tenant)
		my_tenant = getenv("OS_PROJECT_NAME");
	if (!my_url)
		my_url = getenv("OS_AUTH_URL");

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
	case 'T':
		if (ac < 1) {
			goto Usage;
		}
		--ac;
		my_token = *++av;
		break;
	case 't':
		if (ac < 1) {
			goto Usage;
		}
		--ac;
		my_tenant = *++av;
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
	if (!my_user)
		my_user = getenv("OS_USERNAME");
	if (!my_password)
		my_password = getenv("OS_PASSWORD");
	if (!my_tenant)
		my_tenant = getenv("OS_TENANT_NAME");
	if (!my_url)
		my_url = getenv("OS_AUTH_URL");

	if (!change_here)
		;
	else if (chdir(change_here) < 0) {
		fprintf(stderr,"Error doing chdir to %s\n", change_here);
		exit(1);
	}
	curl_global_init(CURL_GLOBAL_DEFAULT);
	init_curl_handles();
	r = process();
	flush_curl_handles();
	curl_global_cleanup();
	exit(r);
}
