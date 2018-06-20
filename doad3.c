#include <stdio.h>
#include <stdlib.h>
#include <curl/curl.h>
#include <pthread.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <json-c/json.h>

extern char *strptime();

char *my_method = "GET";
char *my_user;
char *my_password;
char *my_url;
int multi_count;
int fixed_size;
int nt = 1;
char *capath;
char *my_token;
char *my_project;
char *my_store;
char *my_container;
int vflag;
int Vflag;

char fmt[] = "%FT%T%z";

char *
convert_time(char *p, struct timespec *ts, char *errbuf)
{
	struct tm tb[1];
	char *q, *xx;
	char *x = 0;
	int fr = 0;
	int c = 9;

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
		strcpy(xx, q);
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
		sprintf(errbuf,"failed at offset %d", q-p);
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

int
iterate_json_array(struct json_object *ar, int (*f)(), void *a)
{
	int i, c;
	int r = 0;
	json_object *item;
	if (!json_object_is_type(ar, json_type_array))
		return -1;
	c = json_object_array_length(ar);
	for (i = 0; i < c; ++i) {
		item = json_object_array_get_idx(ar, i);
		r = f(a, item);
		if (r) break;
	}
	return r;
}

struct token_arg {
	struct json_tokener *json_tokeniser;
	char *authtoken;
	const char *storage_url;
	struct timespec expires_at;
};

int
eat_keystone_endpoint(void *h, json_object *ep)
{
	struct token_arg *a = h;
	struct json_object *jo;

#if 0
	const char *endpoint_json = json_object_to_json_string_ext(ep, JSON_C_TO_STRING_PRETTY);
	printf("endpoint: <%s>\n", endpoint_json);
#endif

	if (!json_object_object_get_ex(ep, "interface", &jo)) {
		fprintf(stderr,"Can't find interface in endpoint\n");
		return -1;
	}
	if (!json_object_is_type(jo, json_type_string)) {
		fprintf(stderr,"weird interface in endpoint\n");
		return -1;
	}
	const char *s = json_object_get_string(jo);

	if (strcmp(s, "public"))
		return 0;

	if (!json_object_object_get_ex(ep, "url", &jo)) {
		fprintf(stderr,"Can't find url in endpoint\n");
		return -1;
	}
	if (!json_object_is_type(jo, json_type_string)) {
		fprintf(stderr,"weird url in endpoint\n");
		return -1;
	}
	a->storage_url = json_object_get_string(jo);
	return 0;
}

int
eat_keystone_catalog(void *h, json_object *ep)
{
	struct json_object *jo;
	if (!json_object_object_get_ex(ep, "type", &jo)) {
		fprintf(stderr,"Can't find type in catalog entry\n");
		return -1;
	}
	if (!json_object_is_type(jo, json_type_string)) {
		fprintf(stderr,"weird type in catalog entry\n");
		return -1;
	}
	const char *s = json_object_get_string(jo);

	if (strcmp(s, "object-store"))
		return 0;

	struct json_object *je;
	if (!json_object_object_get_ex(ep, "endpoints", &je)) {
		fprintf(stderr,"Can't find endpoints in catalog entry\n");
		return -1;
	}
	if (!json_object_is_type(je, json_type_array)) {
		fprintf(stderr,"weird endpoints in catalog entry\n");
		return -1;
	}
	return iterate_json_array(je, eat_keystone_endpoint, h);
}

int
eat_expires_at(void *h, json_object *ep)
{
	struct token_arg *a = h;
	char errbuf[512];
	if (json_object_is_type(ep, json_type_null)) {
		// " does not expire "
		memset(&a->expires_at, 0, sizeof a->expires_at);
		return 1;
	} else if (!json_object_is_type(ep, json_type_string)) {
		fprintf(stderr,"weird type in expires_at entry\n");
		return 0;
	}
	const char *s = json_object_get_string(ep);
	char *e = convert_time((char *)s, &a->expires_at, errbuf);
	if (e) {
		fprintf(stderr,"Can't convert time from <%s>: %s\n", s, e);
		return 0;
	}
	return 1;
}

uint
eat_keystone_data(char *in, uint size, uint num, void *h)
{
	uint r;
	r = size * num;
	struct token_arg *a = h;
	json_object *jobj;
	json_object *token, *catalog, *expires_at;
	enum json_tokener_error je;
#if 0
	fprintf(stdout,"Received: ");
	r = fwrite(in, 1, r, stdout);
#endif
	jobj = json_tokener_parse_ex(a->json_tokeniser, in, r);
	je = json_tokener_get_error(a->json_tokeniser);
	if (je != json_tokener_success) {
		fprintf(stderr,"Cannot parse json: e=%d string=<%s>\n",
			r, in);
goto Done;
	}
if (vflag > 1) {
	const char *response_json = json_object_to_json_string_ext(jobj, JSON_C_TO_STRING_PRETTY);
printf ("RECV: <%s>\n", response_json);
	}
	if (!json_object_object_get_ex(jobj, "token", &token)) {
		fprintf(stderr,"Can't find token in response\n");
		goto Done;
	}
	if (!json_object_object_get_ex(token, "expires_at", &expires_at)) {
		fprintf(stderr,"Can't find expires_at in token from response\n");
		goto Done;
	}
	if (!eat_expires_at(h, expires_at)) {
		goto Done;
	}
	if (!json_object_object_get_ex(token, "catalog", &catalog)) {
		fprintf(stderr,"Can't find catalog in token from response\n");
		goto Done;
	}
	iterate_json_array(catalog, eat_keystone_catalog, h);
Done:
	return r;
}

// XXX in the case of redirects, should accept x-subject-token only from
//  LAST response.  How?
size_t
eat_keystone_header(char *buffer, uint size, uint num, void *h)
{
	uint r;
	static char subjecttoken[] = "X-Subject-Token:";
	r = size * num;
	struct token_arg *a = h;
	if (!strncasecmp(buffer, subjecttoken, sizeof subjecttoken-1)) {
		char *cp = malloc(r + 5);
		char *inp = buffer; uint s = r;
		inp += sizeof subjecttoken-1;
		s -= sizeof subjecttoken-1;
		while (s && *inp == ' ') {
			--s; ++inp;
		}
		if (s) {
			memcpy(cp, inp, s);
			cp[s] = 0;
			inp = strchr(cp, '\r');
			if (inp) *inp = 0;
			inp = strchr(cp, '\n');
			if (inp) *inp = 0;
			a->authtoken = cp;
		}
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
	json_object *jdom = json_object_new_object();
	json_object *jobj = json_object_new_object();
	json_object *jproj = json_object_new_object();

	temp_url = malloc(i = strlen(my_url) + 30);
	snprintf(temp_url, i, "%s/auth/tokens", my_url);
	json_object_object_add(jdom, "name", json_object_new_string("Default"));
	json_object_array_add(jm, json_object_new_string("password"));
//	json_object_object_add(jx, "name", json_object_new_string("Default"));
//	json_object_object_add(jpu, "domain", jx);
	json_object_object_add(jpu, "domain", jdom);
	json_object_object_add(jproj, "name", json_object_new_string(my_project));
	json_object_object_add(jproj, "domain", jdom);
	json_object_object_add(jx, "project", jproj);
	json_object_object_add(jpu, "name", json_object_new_string(my_user));
	json_object_object_add(jpu, "password", json_object_new_string(my_password));
	json_object_object_add(jy, "user", jpu);
	json_object_object_add(jt, "password", jy);
	json_object_object_add(jt, "methods", jm);
	json_object_object_add(jz, "identity", jt);
	json_object_object_add(jz, "scope", jx);
	json_object_object_add(jobj, "auth", jz);

	const char *req_json = json_object_to_json_string_ext(jobj, JSON_C_TO_STRING_PLAIN);
	memset(recvarg, 0, sizeof *recvarg);
	recvarg->json_tokeniser = json_tokener_new();
if (vflag > 1) printf ("SEND: <%s>\n", req_json);
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
	curl_easy_setopt(curl, CURLOPT_HEADERDATA, (void*)recvarg);
	curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, eat_keystone_header);
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
	if (recvarg->authtoken) {
if (vflag) printf ("got auth-token: <%s>\n", recvarg->authtoken);
		my_token = strdup(recvarg->authtoken);	// how to free?
	} else {
		fprintf(stderr,"Failed to receive auth token\n");
		r = 1;
	}
	if (recvarg->storage_url) {
if (vflag) printf ("got storage url: <%s>\n", recvarg->storage_url);
		my_store = strdup(recvarg->storage_url);	// free?
	} else {
		fprintf(stderr,"Failed to find storage service\n");
		r = 1;
	}
	if (recvarg->expires_at.tv_sec) {
		struct timespec now[1], dur[1];
		if (clock_gettime(CLOCK_REALTIME, now) < 0) {
			fprintf(stderr,"gettime failed %d\n", errno);
			fprintf(stderr,"Token expires at %d.%09d seconds\n",
			recvarg->expires_at.tv_sec,
			recvarg->expires_at.tv_nsec);
		} else {
			dur->tv_sec = recvarg->expires_at.tv_sec - now->tv_sec;
			dur->tv_nsec = recvarg->expires_at.tv_nsec - now->tv_nsec;
			if (dur->tv_nsec < 0) {
				dur->tv_sec -= 1;
				dur->tv_nsec += 1000000000;
			}
			fprintf(stderr,"token expires in %d.%09d seconds\n",
				dur->tv_sec,
				dur->tv_nsec);
		}
	} else {
		fprintf(stderr,"token does not expire\n");
	}
	return r;
}

pthread_mutex_t exists_mutex;
pthread_cond_t exists_cond;
#define NH 55
struct exists_entry {
	struct exists_entry *next;
	int exists;
	int want;
	char fn[1];
} *exists_hash[NH];

int
compute_exists_hash(char *fn)
{
	unsigned r = 0;
	char *cp;
	for (cp = fn; *cp; ++cp) {
		r *= 5;
		r ^= *cp;
	}
	r %= NH;
	return (int) r;
}

void
wait_until_exists(char *fn)
{
	int nh;
	nh = compute_exists_hash(fn);
	struct exists_entry *ep, **epp;
	if (pthread_mutex_lock(&exists_mutex) < 0) {
		fprintf(stderr,"lock failed %d\n", errno);
	}
	for (;;) {
		for (epp = exists_hash + nh; ep = *epp; epp = &ep->next) {
			if (!strcmp(ep->fn, fn)) break;
		}
		if (!ep) {
			ep = malloc(sizeof *ep + strlen(fn));
			memset(ep, 0, sizeof *ep);
			strcpy(ep->fn, fn);
			ep->want = 1;
			ep->next = *epp;
			*epp = ep;
			continue;
		}
		if (ep->exists) break;
		if (pthread_cond_wait(&exists_cond, &exists_mutex) < 0) {
			fprintf(stderr,"cond wait failed %d\n", errno);
		}
	}
	if (pthread_mutex_unlock(&exists_mutex) < 0) {
		fprintf(stderr,"unlock failed %d\n", errno);
	}
}

void
mark_it_exists(char *fn)
{
	int nh;
	nh = compute_exists_hash(fn);
	struct exists_entry *ep, **epp;

	if (pthread_mutex_lock(&exists_mutex) < 0) {
		fprintf(stderr,"lock failed %d\n", errno);
	}
	for (epp = exists_hash + nh; ep = *epp; epp = &ep->next) {
		if (!strcmp(ep->fn, fn)) break;
	}
	if (!ep) {
		ep = malloc(sizeof *ep + strlen(fn));
		memset(ep, 0, sizeof *ep);
		strcpy(ep->fn, fn);
		ep->next = *epp;
		*epp = ep;
	}
	ep->exists = 1;
	if (ep->want) {
		pthread_cond_broadcast(&exists_cond);
	}
	if (pthread_mutex_unlock(&exists_mutex) < 0) {
		fprintf(stderr,"unlock failed %d\n", errno);
	}
}

#define W_ADD 1
#define W_DEL 2
struct work_element {
	int op;
	char *what;
	struct work_element *next;
};

struct work_element *work_queue;
pthread_mutex_t work_mutex;

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
		wp->what = cp;
		strcpy(cp, what);
		*wpp = wp;
		wpp = &wp->next;
	}
	return r;
}
struct make_data_arg {
	FILE *fp;
	int off;
};


uint
make_data_function(char *in, uint size, uint num, void *h)
{
	struct make_data_arg *a = h;
	uint r;
	int c;
	if (a->fp)
		r = fread(in, size, num, a->fp);
	else {
		r = size * num;
		c = fixed_size - a->off;
		if (r > c) r = c;
		memset(in, 0, r);
		a->off += r;
	}
	return r;
}


uint
ignore_data_function(char *in, uint size, uint num, void *h)
{
	uint r;
	r = size * num;
	return r;
}

pthread_t *worker_ids;

struct worker_result {
	int r;
};

void *
worker_thread(void *a)
{
	struct curl_carrier *ca;
	CURL *curl;
	struct curl_slist *headers;
	CURLcode rc;
	char error_buf[CURL_ERROR_SIZE];
	long http_status;
	int len_url;
	int len_xauth;
	int len_timestamp;
	int r;
	struct worker_result *wr;
	struct work_element *wp;
	struct work_element **wpp;
	char *temp_url;
	char *temp_xauth;
	char temp_timestamp[80];
	char timestamp_formatted[60];
	CURLoption opt;
	struct make_data_arg makedataarg[1];
	int first;
	struct stat stbuf;

	wr = malloc(sizeof *wr);
	ca = get_curl_handle();
	curl = ca->h;
	temp_url = malloc(len_url = strlen(my_store) + 80);
	temp_xauth = malloc(len_xauth = strlen(my_token) + 80);

	for (first = 1;;first = 0) {
		headers = 0;
		if (pthread_mutex_lock(&work_mutex) < 0) {
			fprintf(stderr,"lock failed %d\n", errno);
		}
		for (wpp = & work_queue; wp = *wpp; ) {
			*wpp = wp->next;
			break;
		}
		if (pthread_mutex_unlock(&work_mutex) < 0) {
			fprintf(stderr,"unlock failed %d\n", errno);
		}
		if (!wp) break;
		if (wp->op == W_DEL) {
			wait_until_exists(wp->what);
		}
		if (!first)
			curl_easy_reset(curl);

		snprintf(temp_xauth, len_xauth, "X-Auth-Token: %s", my_token);
		headers = curl_slist_append(headers, temp_xauth);
//		headers = curl_slist_append(headers, "Content-Type: " "application/json");
		curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
		curl_easy_setopt(curl, CURLOPT_NOPROGRESS, 1L);
		curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
		curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, error_buf);
//		curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void*)recvarg);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, ignore_data_function);

		snprintf (temp_url, len_url, "%s/%s/%s", my_store, my_container, wp->what);
		memset(makedataarg, 0, sizeof *makedataarg);
		switch (wp->op) {
		case W_DEL:
if (vflag) printf ("deleting %s\n", wp->what);
			curl_easy_setopt(curl, CURLOPT_POSTFIELDS, NULL);
			curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, 0);
			curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
			break;
		case W_ADD:
if (vflag) printf ("adding %s\n", wp->what);
			if (fixed_size) {
				clock_gettime(CLOCK_REALTIME, &stbuf.st_mtim);
			} else {
				makedataarg->fp = fopen(wp->what, "r");
				if (!makedataarg->fp) {
					fprintf(stderr,"Can't open %s\n", wp->what);
					continue;
				}
				if (fstat(fileno(makedataarg->fp), &stbuf) < 0) {
					fprintf(stderr,"Can't stat %s\n", wp->what);
					continue;
				}
			}
			snprintf(temp_timestamp, sizeof temp_timestamp,
				"X-object-meta-mtime: %s",
					fix_format_microseconds(
					format_timespec(timestamp_formatted, sizeof timestamp_formatted,
					&stbuf.st_mtim)));
			headers = curl_slist_append(headers, temp_timestamp);
			curl_easy_setopt(curl, CURLOPT_READFUNCTION, make_data_function);
			curl_easy_setopt(curl, CURLOPT_READDATA, makedataarg);
			curl_easy_setopt(curl, CURLOPT_UPLOAD, 1);
			break;
		default:
			fprintf(stderr,"op %d? for fn %s\n", wp->op, wp->what);
			continue;
		}
		curl_easy_setopt(curl, CURLOPT_URL, temp_url);
		// DO IT HERE
		rc = curl_easy_perform(curl);
		if (rc != CURLE_OK) {
			fprintf(stderr,"curl_easy_perform failed, %s\n",
				curl_easy_strerror(rc));
			r |= 2;
			goto Next;
		}
		curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_status);
		if (makedataarg->fp)
			fclose(makedataarg->fp);
		if (http_status < 200 || http_status > 299) {
			fprintf (stderr,"While %s on %s: got %d\n",
				wp->op==W_ADD ? "adding" : "deleting",
				wp->what,
				(int) http_status);
		}
	Next:
		if (wp->op == W_ADD) {
			mark_it_exists(wp->what);
		}
		curl_slist_free_all(headers);
		free(wp);
	}
Done:
	release_curl_handle(ca);
	free(temp_url);
	free(temp_xauth);
	if (r) {
		wr->r = r;
	} else {
		free(wr);
		wr = 0;
	}
	return wr;
}

void
start_threads()
{
	int i;
	worker_ids = malloc(nt * sizeof *worker_ids);
	for (i = 0; i < nt; ++i)
		pthread_create(worker_ids + i, NULL, worker_thread, NULL);
}

void
wait_for_completion()
{
	int i;
	void *result;
	int r;
	struct worker_result *wr;
	r = 0;
	for (i = 0; i < nt; ++i) {
		if (pthread_join(worker_ids[i], &result) < 0) {
			fprintf(stderr,"pthread_join failed %d\n", errno);
			return;
		}
		wr = result;
		if (wr) {
			r |= wr->r;
			free(wr);
		}
	}
	free(worker_ids);
	worker_ids = 0;
}

void
report_results()
{
}

void
free_other_stuff()
{
	// free my_store sometimes?
	// free my_token sometimes?
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
	if (!my_project)
		my_project = getenv("OS_PROJECT_NAME");
	if (!my_url)
		my_url = getenv("OS_AUTH_URL");

	while (--ac > 0) if (*(ap = *++av) == '-') while (*++ap) switch(*ap) {
	case 'v':
		++vflag;
		break;
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
	case 'S':
		if (ac < 1) {
			goto Usage;
		}
		--ac;
		my_store = *++av;
		break;
	case 'T':
		if (ac < 1) {
			goto Usage;
		}
		--ac;
		my_token = *++av;
		break;
	case 'P':
		if (ac < 1) {
			goto Usage;
		}
		--ac;
		my_project = *++av;
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
	case 's':
		if (ac < 1) {
			goto Usage;
		}
		--ac;
		cp = *++av;
		fixed_size = strtoll(cp, &ep, 0);
		if (cp == ep || *ep) {
			fprintf(stderr,"Bad multicount <%s>\n", cp);
			goto Usage;
		}
		break;
	case 'b':
		if (ac < 1) {
			goto Usage;
		}
		--ac;
		my_container = *++av;
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
	case 't':
		if (ac < 1) {
			goto Usage;
		}
		--ac;
		cp = *++av;
		nt = strtoll(cp, &ep, 0);
		if (cp == ep || *ep) {
			fprintf(stderr,"Can't parse thread count <%s>\n", cp);
			goto Usage;
		}
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
		fprintf(stderr,"Usage: doad3 [-u user] [-p pass] [-P proj] [-t #threads] [-h hosturl] [-C capath] [-V] [-s size] -b container\n");
		exit(1);
	} else if (!my_url) {
		my_url = ap;
	} else {
		fprintf(stderr,"extra arg?\n");
		goto Usage;
	}
	if (nt <= 0) {
		fprintf(stderr,"Bad thread count %d\n", nt);
		goto Usage;
	}
	if (!my_container) {
		fprintf(stderr,"must specific -b container\n");
		goto Usage;
	}
	if (!my_user)
		my_user = getenv("OS_USERNAME");
	if (!my_password)
		my_password = getenv("OS_PASSWORD");
	if (!my_project)
		my_project = getenv("OS_PROJECT_NAME");
	if (!my_url)
		my_url = getenv("OS_AUTH_URL");
	if (!my_store)
		my_store = getenv("OS_STORAGE_URL");
	if (!my_token)
		my_token = getenv("OS_AUTH_TOKEN");

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
	free_other_stuff();
	exit(r);
}
