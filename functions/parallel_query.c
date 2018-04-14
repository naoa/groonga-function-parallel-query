/* 
  Copyright(C) 2016 Naoya Murakami <naoya@createfield.com>

  This library is free software; you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public
  License version 2.1 as published by the Free Software Foundation.

  This library is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public
  License along with this library; if not, write to the Free Software
  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
*/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <groonga/plugin.h>

#ifdef __GNUC__
# define GNUC_UNUSED __attribute__((__unused__))
#else
# define GNUC_UNUSED
#endif

int n_worker = 8;

/* copy from lib/grn_rset.h */

#define GRN_RSET_UTIL_BIT (0x80000000)

typedef struct {
  double score;
  int n_subrecs;
  int subrecs[1];
} grn_rset_recinfo;

typedef struct {
  grn_id rid;
  uint32_t sid;
  uint32_t pos;
} grn_rset_posinfo;
/* lib/grn_rset.h */

#define GRN_CTX_TEMPORARY_DISABLE_II_RESOLVE_SEL_AND (0x40)

/* copy form lib/ii.c */

static void
grn_ii_resolve_sel_and_(grn_ctx *ctx, grn_hash *s, grn_operator op)
{
  if (op == GRN_OP_AND
      && !(ctx->flags & GRN_CTX_TEMPORARY_DISABLE_II_RESOLVE_SEL_AND)) {
    grn_id eid;
    grn_rset_recinfo *ri;
    grn_hash_cursor *c = grn_hash_cursor_open(ctx, s, NULL, 0, NULL, 0,
                                              0, -1, 0);
    if (c) {
      while ((eid = grn_hash_cursor_next(ctx, c))) {
        grn_hash_cursor_get_value(ctx, c, (void **) &ri);
        if ((ri->n_subrecs & GRN_RSET_UTIL_BIT)) {
          ri->n_subrecs &= ~GRN_RSET_UTIL_BIT;
        } else {
          grn_hash_delete_by_id(ctx, s, eid, NULL);
        }
      }
      grn_hash_cursor_close(ctx, c);
    }
  }
}

static void
replace_char(char *name, int len, char from, char to)
{
  char *p = (char *)name;
  int i;
  for (i = 0; i < len; i++) {
    if (p[0] == from) {
      p[0] = to;
    }
    p++;
  }
}

static grn_rc
grn_table_res_add(grn_ctx *ctx, grn_obj *thread_res, grn_obj *res,
                  grn_operator op)
{
  grn_rc rc = GRN_SUCCESS;
  grn_table_cursor *cur = NULL;

  cur = grn_table_cursor_open(ctx, thread_res, NULL, 0, NULL, 0, 0, -1, GRN_CURSOR_BY_ID);
  if (!cur) {
    goto exit;
  }

  switch (op) {
  case GRN_OP_OR :
    {
      grn_id id;
      while ((id = grn_table_cursor_next(ctx, cur)) != GRN_ID_NIL) {
        int added;
        void *key;
        unsigned int key_size;
        grn_rset_recinfo *ri_res;
        grn_rset_recinfo *ri_thread_res;
        grn_hash_cursor_get_key_value(ctx, (grn_hash_cursor *)cur, &key, &key_size, (void **)&ri_thread_res);
        if (grn_hash_add(ctx, (grn_hash *)res, key, key_size, (void **)&ri_res, &added)) {
          if (added) {
            ri_res->score = ri_thread_res->score;
            ri_res->n_subrecs = 1;
          } else {
            ri_res->score += ri_thread_res->score;
            ri_res->n_subrecs += 1;
          }
        }
      }
    }
    break;
  case GRN_OP_AND :
    {
      grn_id id;
      while ((id = grn_table_cursor_next(ctx, cur)) != GRN_ID_NIL) {
        void *key;
        unsigned int key_size;
        grn_rset_recinfo *ri_res;
        grn_rset_recinfo *ri_thread_res;
        grn_hash_cursor_get_key_value(ctx, (grn_hash_cursor *)cur, &key, &key_size, (void **)&ri_thread_res);
        if (grn_hash_get(ctx, (grn_hash *)res, key, key_size, (void **)&ri_res)) {
          ri_res->score += ri_thread_res->score;
          ri_res->n_subrecs += 1;
          ri_res->n_subrecs |= GRN_RSET_UTIL_BIT;
        }
      }
    }
    break;
  case GRN_OP_AND_NOT :
    {
      grn_id id;
      while ((id = grn_table_cursor_next(ctx, cur)) != GRN_ID_NIL) {
        void *key;
        unsigned int key_size;
        grn_rset_recinfo *ri_thread_res;
        if (grn_hash_cursor_get_key_value(ctx, (grn_hash_cursor *)cur, &key, &key_size, (void **)&ri_thread_res)) {
          grn_table_delete(ctx, res, key, key_size);
        }
      }
    }
    break;
  case GRN_OP_ADJUST :
    {
      grn_id id;
      while ((id = grn_table_cursor_next(ctx, cur)) != GRN_ID_NIL) {
        void *key;
        unsigned int key_size;
        grn_rset_recinfo *ri_res;
        grn_rset_recinfo *ri_thread_res;
        grn_hash_cursor_get_key_value(ctx, (grn_hash_cursor *)cur, &key, &key_size, (void **)&ri_thread_res);
        if (grn_hash_get(ctx, (grn_hash *)res, key, key_size, (void **)&ri_res)) {
          ri_res->score += ri_thread_res->score;
        }
      }
    }
    break;
  default :
    break;
  }

exit: 
  if (cur) {
    grn_table_cursor_close(ctx, cur);
  }
  return rc;
}

static grn_rc
grn_sorted_res_add(grn_ctx *ctx, grn_obj *sorted, grn_obj *thread_res, grn_obj *res,
                   grn_operator op)
{
  grn_rc rc = GRN_SUCCESS;
  grn_table_cursor *cur = NULL;
  grn_obj *score_column = NULL;
  grn_obj score_value;
  GRN_FLOAT_INIT(&score_value, 0);

  score_column = grn_obj_column(ctx, thread_res,
                                GRN_COLUMN_NAME_SCORE,
                                GRN_COLUMN_NAME_SCORE_LEN);

  if (!score_column) {
    goto exit;
  }

  cur = grn_table_cursor_open(ctx, sorted, NULL, 0, NULL, 0, 0, -1, GRN_CURSOR_BY_ID);
  if (!cur) {
    goto exit;
  }

  switch (op) {
  case GRN_OP_OR :
    {
      grn_id id;
      while ((id = grn_table_cursor_next(ctx, cur)) != GRN_ID_NIL) {
        int added;
        grn_id *thread_res_id;
        grn_id rid;
        grn_rset_recinfo *ri_res;
        grn_table_cursor_get_value(ctx, cur, (void **)&thread_res_id);
        grn_hash_get_key(ctx, (grn_hash *)thread_res, *thread_res_id, &rid, sizeof(grn_id));
        GRN_BULK_REWIND(&score_value);
        grn_obj_get_value(ctx, score_column, *thread_res_id, &score_value);
        if (grn_hash_add(ctx, (grn_hash *)res, &rid, sizeof(grn_id), (void **)&ri_res, &added)) {
          if (added) {
            ri_res->score = GRN_FLOAT_VALUE(&score_value);
            ri_res->n_subrecs = 1;
          } else {
            ri_res->score += GRN_FLOAT_VALUE(&score_value);
            ri_res->n_subrecs += 1;
          }
        }
      }
    }
    break;
  case GRN_OP_AND :
    {
      grn_id id;
      while ((id = grn_table_cursor_next(ctx, cur)) != GRN_ID_NIL) {
        grn_id *thread_res_id;
        grn_id rid;
        grn_rset_recinfo *ri_res;
        grn_table_cursor_get_value(ctx, cur, (void **)&thread_res_id);
        grn_hash_get_key(ctx, (grn_hash *)thread_res, *thread_res_id, &rid, sizeof(grn_id));
        GRN_BULK_REWIND(&score_value);
        grn_obj_get_value(ctx, score_column, *thread_res_id, &score_value);
        if (grn_hash_get(ctx, (grn_hash *)res, &rid, sizeof(grn_id), (void **)&ri_res)) {
          ri_res->score += GRN_FLOAT_VALUE(&score_value);
          ri_res->n_subrecs += 1;
          ri_res->n_subrecs |= GRN_RSET_UTIL_BIT;
        }
      }
    }
    break;
  case GRN_OP_AND_NOT :
    {
      grn_id id;
      while ((id = grn_table_cursor_next(ctx, cur)) != GRN_ID_NIL) {
        grn_id *thread_res_id;
        grn_id rid;
        grn_table_cursor_get_value(ctx, cur, (void **)&thread_res_id);
        grn_hash_get_key(ctx, (grn_hash *)thread_res, *thread_res_id, &rid, sizeof(grn_id));
        grn_table_delete(ctx, res, &rid, sizeof(grn_id));
      }
    }
    break;
  case GRN_OP_ADJUST :
    {
      grn_id id;
      while ((id = grn_table_cursor_next(ctx, cur)) != GRN_ID_NIL) {
        grn_id *thread_res_id;
        grn_id rid;
        grn_rset_recinfo *ri_res;
        grn_table_cursor_get_value(ctx, cur, (void **)&thread_res_id);
        grn_hash_get_key(ctx, (grn_hash *)thread_res, *thread_res_id, &rid, sizeof(grn_id));
        GRN_BULK_REWIND(&score_value);
        grn_obj_get_value(ctx, score_column, *thread_res_id, &score_value);
        if (grn_hash_get(ctx, (grn_hash *)res, &rid, sizeof(grn_id), (void **)&ri_res)) {
          ri_res->score += GRN_FLOAT_VALUE(&score_value);
        }
      }
    }
    break;
  default :
    break;
  }

exit: 
  if (cur) {
    grn_table_cursor_close(ctx, cur);
  }
  GRN_OBJ_FIN(ctx, &score_value);
  if (score_column) {
    grn_obj_unlink(ctx, score_column);
  }
  return rc;
}

typedef struct {
  grn_obj *db;
  grn_obj *table;
  grn_obj *res;
  const char *match_columns_string;
  int match_columns_string_length;
  grn_obj *query;
  grn_operator op;
  grn_bool main;
  unsigned int top_n;
  const char *top_n_sort_keys;
  unsigned int top_n_sort_keys_length;
  pthread_mutex_t *m;
} thread_query_args;

static void*
thread_query(void *p)
{
  thread_query_args *ip = (thread_query_args *)p;
  grn_ctx ctx_;
  grn_ctx *ctx = &ctx_;
  grn_obj *table = ip->table;
  grn_obj *res = ip->res;
  const char *match_columns_string = ip->match_columns_string;
  int match_columns_string_length = ip->match_columns_string_length;
  grn_obj *query = ip->query;
  grn_operator op = ip->op;
  unsigned int top_n = ip->top_n;
  const char *top_n_sort_keys = ip->top_n_sort_keys;
  unsigned int top_n_sort_keys_length = ip->top_n_sort_keys_length;

  grn_rc rc = GRN_SUCCESS;
  grn_obj *match_columns = NULL;
  grn_obj *condition = NULL;
  grn_obj *dummy_variable;
  grn_obj *thread_res = NULL;
  int ret;

  rc = grn_ctx_init(ctx, 0);
  if (rc != GRN_SUCCESS) {
    goto exit;
  }
  grn_ctx_use(ctx, ip->db);

  if (match_columns_string_length > 0) {

    GRN_EXPR_CREATE_FOR_QUERY(ctx, table, match_columns, dummy_variable);
    if (!match_columns) {
      rc = ctx->rc;
      goto exit;
    }

    grn_expr_parse(ctx, match_columns,
                   match_columns_string,
                   match_columns_string_length,
                   NULL, GRN_OP_MATCH, GRN_OP_AND,
                   GRN_EXPR_SYNTAX_SCRIPT);
    if (ctx->rc != GRN_SUCCESS) {
      rc = ctx->rc;
      goto exit;
    }
  }

  if (query->header.domain == GRN_DB_TEXT && GRN_TEXT_LEN(query) > 0) {
    const char *query_string;
    unsigned int query_string_len;
    grn_expr_flags flags =
      GRN_EXPR_SYNTAX_QUERY|GRN_EXPR_ALLOW_PRAGMA|GRN_EXPR_ALLOW_COLUMN;

    GRN_EXPR_CREATE_FOR_QUERY(ctx, table, condition, dummy_variable);
    if (!condition) {
      rc = ctx->rc;
      goto exit;
    }

    query_string = GRN_TEXT_VALUE(query);
    query_string_len = GRN_TEXT_LEN(query);

    grn_expr_parse(ctx, condition,
                   query_string,
                   query_string_len,
                   match_columns, GRN_OP_MATCH, GRN_OP_AND, flags);
    rc = ctx->rc;
    if (rc != GRN_SUCCESS) {
      goto exit;
    }

    ctx->flags |= GRN_CTX_TEMPORARY_DISABLE_II_RESOLVE_SEL_AND;
    if (ip->main) {
      ret = pthread_mutex_lock(ip->m);
      if (ret != 0) {
        rc = GRN_NO_LOCKS_AVAILABLE;
        goto exit;
      }
      grn_table_select(ctx, table, condition, res, op);
      rc = ctx->rc;
      ret = pthread_mutex_unlock(ip->m);
      if (ret != 0) {
        rc = GRN_NO_LOCKS_AVAILABLE;
        goto exit;
      }
    } else {
      unsigned int n_hits;
      thread_res = grn_table_create(ctx, NULL, 0, NULL,
                                    GRN_TABLE_HASH_KEY|GRN_OBJ_WITH_SUBREC,
                                    table, NULL);
      if (!thread_res) {
        rc = ctx->rc;
        goto exit;
      }
      grn_table_select(ctx, table, condition, thread_res, GRN_OP_OR);
      rc = ctx->rc;

      n_hits = grn_table_size(ctx, thread_res);
      if (n_hits > 0) {
        if (top_n == 0 || n_hits <= top_n) {
          ret = pthread_mutex_lock(ip->m);
          if (ret != 0) {
            rc = GRN_NO_LOCKS_AVAILABLE;
            goto exit;
          }
          rc = grn_table_res_add(ctx, thread_res, res, op);
          ret = pthread_mutex_unlock(ip->m);
          if (ret != 0) {
            rc = GRN_NO_LOCKS_AVAILABLE;
            goto exit;
          }
        } else {
          grn_obj *sorted;
          if ((sorted = grn_table_create(ctx, NULL, 0, NULL, GRN_OBJ_TABLE_NO_KEY, NULL, thread_res))) {
            uint32_t nkeys;
            grn_table_sort_key *keys;
            if ((keys = grn_table_sort_key_from_str(ctx, top_n_sort_keys, top_n_sort_keys_length, thread_res, &nkeys))) {
              grn_table_sort(ctx, thread_res, 0, top_n, sorted, keys, nkeys);
              ret = pthread_mutex_lock(ip->m);
              if (ret != 0) {
                rc = GRN_NO_LOCKS_AVAILABLE;
                goto exit;
              }
              rc = grn_sorted_res_add(ctx, sorted, thread_res, res, op);
              ret = pthread_mutex_unlock(ip->m);
              if (ret != 0) {
                rc = GRN_NO_LOCKS_AVAILABLE;
                goto exit;
              }
              grn_table_sort_key_close(ctx, keys, nkeys);
            }
            grn_obj_unlink(ctx, sorted);
          }
        }
      }
    }
  }

exit :
  if (match_columns) {
    grn_obj_unlink(ctx, match_columns);
  }
  if (condition) {
    grn_obj_unlink(ctx, condition);
  }
  if (thread_res) {
    grn_obj_unlink(ctx, thread_res);
  }
  grn_ctx_fin(ctx);
  /* should be return error code */
  pthread_exit(NULL);
}
 
static grn_rc
run_parallel_query(grn_ctx *ctx, grn_obj *table,
                   int nargs, grn_obj **args,
                   grn_obj *res, grn_operator op)
{
  grn_rc rc = GRN_SUCCESS;
  int i, t, n = 0;
  int ret;
  int n_query_args = nargs;
  pthread_t threads[n_worker]; /* should be malloc */
  thread_query_args qa[n_worker]; 
  grn_obj *db = grn_ctx_db(ctx);
  grn_obj *options;
  grn_bool separate_query = GRN_FALSE;
  unsigned int top_n = 0;
  const char *top_n_sort_keys = "-_score";
  unsigned int top_n_sort_keys_length = 7;
  int digit_format = 0;
  grn_obj *merge_res = NULL;
  pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;

  if (nargs < 2) {
    GRN_PLUGIN_ERROR(ctx, GRN_INVALID_ARGUMENT,
                     "wrong number of arguments (%d for 2..)", nargs);
    rc = ctx->rc;
    goto exit;
  }

  options = args[nargs - 1];
  if (options->header.type == GRN_TABLE_HASH_KEY) {
    grn_hash_cursor *cursor;
    void *key;
    grn_obj *value;
    unsigned int key_size;
    n_query_args--;
    cursor = grn_hash_cursor_open(ctx, (grn_hash *)options,
                                  NULL, 0, NULL, 0,
                                  0, -1, 0);
    if (!cursor) {
      GRN_PLUGIN_ERROR(ctx, GRN_NO_MEMORY_AVAILABLE,
                       "parallel_query(): couldn't open cursor");
      goto exit;
    }
    while (grn_hash_cursor_next(ctx, cursor) != GRN_ID_NIL) {
      grn_hash_cursor_get_key_value(ctx, cursor, &key, &key_size,
                                    (void **)&value);

      if (key_size == 8 && !memcmp(key, "separate", 8)) {
        separate_query = GRN_TRUE;
      } else if (key_size == 5 && !memcmp(key, "top_n", 5)) {
        top_n = GRN_UINT32_VALUE(value);
      } else if (key_size == 15 && !memcmp(key, "top_n_sort_keys", 15)) {
        top_n_sort_keys = GRN_TEXT_VALUE(value);
        top_n_sort_keys_length = GRN_TEXT_LEN(value);
      } else if (key_size == 12 && !memcmp(key, "digit_format", 12)) {
        digit_format = GRN_INT32_VALUE(value);
      } else {
        GRN_PLUGIN_ERROR(ctx, GRN_INVALID_ARGUMENT,
                         "invalid option name: <%.*s>",
                         key_size, (char *)key);
        grn_hash_cursor_close(ctx, cursor);
        goto exit;
      }
    }
    grn_hash_cursor_close(ctx, cursor);
  }

  if (top_n > 0 && op != GRN_OP_OR) {
    top_n = 0;
  }

  if (top_n > 0) {
    merge_res = grn_table_create(ctx, NULL, 0, NULL,
                                 GRN_TABLE_HASH_KEY|GRN_OBJ_WITH_SUBREC,
                                 table, NULL);
    if (!merge_res) {
      rc = ctx->rc;
      goto exit;
    }
  }

  grn_obj match_columns_strings;
  GRN_TEXT_INIT(&match_columns_strings, GRN_OBJ_VECTOR);

  if (!separate_query) {
    for (i = 0; i < n_query_args - 1; i++) {
      grn_vector_add_element(ctx, &match_columns_strings, GRN_TEXT_VALUE(args[i]), GRN_TEXT_LEN(args[i]), 0, GRN_DB_TEXT);
    }
  } else {
#define QUERY_SET_SIZE 2
    for (i = 0; i + QUERY_SET_SIZE <= n_query_args; i += QUERY_SET_SIZE) {
      grn_vector_add_element(ctx, &match_columns_strings, GRN_TEXT_VALUE(args[i]), GRN_TEXT_LEN(args[i]), 0, GRN_DB_TEXT);
    }
#undef QUERY_SET_SIZE
  }
  if (digit_format > 0) {
    grn_obj parsed_match_columns_strings;
    GRN_TEXT_INIT(&parsed_match_columns_strings, GRN_OBJ_VECTOR);
    for (i = 0; i < (int)grn_vector_size(ctx, &match_columns_strings); i++) {
      const char *match_columns_string;
      int match_columns_string_length;
      grn_obj str_buf;
      match_columns_string_length = grn_vector_get_element(ctx, &match_columns_strings, i, &match_columns_string, NULL, NULL);
      GRN_TEXT_INIT(&str_buf, 0);
      GRN_TEXT_SET(ctx, &str_buf, match_columns_string, match_columns_string_length);
      if (strchr(match_columns_string, '%')) {
        int j;
        for (j = 0; j < digit_format; j++) {
          GRN_BULK_REWIND(&str_buf);
          GRN_TEXT_SET(ctx, &str_buf, match_columns_string, match_columns_string_length);
          replace_char(GRN_TEXT_VALUE(&str_buf), GRN_TEXT_LEN(&str_buf), '%', '1' + j);
          grn_vector_add_element(ctx, &parsed_match_columns_strings, GRN_TEXT_VALUE(&str_buf), GRN_TEXT_LEN(&str_buf), 0, GRN_DB_TEXT);
        }
      } else {
        grn_vector_add_element(ctx, &parsed_match_columns_strings, GRN_TEXT_VALUE(&str_buf), GRN_TEXT_LEN(&str_buf), 0, GRN_DB_TEXT);
      }
      GRN_OBJ_FIN(ctx, &str_buf);
    }
    GRN_BULK_REWIND(&match_columns_strings);
    for (i = 0; i < (int)grn_vector_size(ctx, &parsed_match_columns_strings); i++) {
      const char *match_columns_string;
      int match_columns_string_length;
      match_columns_string_length = grn_vector_get_element(ctx, &parsed_match_columns_strings, i, &match_columns_string, NULL, NULL);
      grn_vector_add_element(ctx, &match_columns_strings, match_columns_string, match_columns_string_length, 0, GRN_DB_TEXT);
    }
    GRN_OBJ_FIN(ctx, &parsed_match_columns_strings);
  }

  if (!separate_query) {
    int col_size = grn_vector_size(ctx, &match_columns_strings);
    for (i = 0; i < col_size; i++) {
      const char *match_columns_string;
      int match_columns_string_length;
      qa[n].db = db;
      qa[n].table = table;
      qa[n].res = merge_res ? merge_res : res;
      match_columns_string_length = grn_vector_get_element(ctx, &match_columns_strings, i, &match_columns_string, NULL, NULL);
      qa[n].match_columns_string = match_columns_string;
      qa[n].match_columns_string_length = match_columns_string_length;
      qa[n].query = args[n_query_args - 1];
      qa[n].top_n = top_n;
      qa[n].top_n_sort_keys = top_n_sort_keys;
      qa[n].top_n_sort_keys_length = top_n_sort_keys_length;
      qa[n].op = op;
      qa[n].main = (n == 0 && top_n == 0 && op == GRN_OP_OR) ? GRN_TRUE : GRN_FALSE;
      qa[n].m = &m;

      ret = pthread_create(&threads[n], NULL, (void *)thread_query, (void *) &qa[n]);
      if (ret != 0) {
        GRN_PLUGIN_ERROR(ctx, GRN_NO_MEMORY_AVAILABLE,
                         "[parallel_query] failed to create pthread");
        rc = ctx->rc;
        goto exit;
      }
      n++;
      if (n == n_worker || i >= n_query_args - 2) {
        for (t = 0; t < n; t++) {
          ret = pthread_join(threads[t], NULL);
          if (ret != 0) {
            GRN_PLUGIN_ERROR(ctx, GRN_NO_MEMORY_AVAILABLE,
                             "[parallel_query] failed to join pthread");
            rc = ctx->rc;
            goto exit;
          } else if (rc != GRN_SUCCESS) {
            goto exit;
          }
        }
        n = 0;
      }
    }
  } else {
#define QUERY_SET_SIZE 2
    int j = 0; 
    for (i = 0; i + QUERY_SET_SIZE <= n_query_args; i += QUERY_SET_SIZE, j++) {
      const char *match_columns_string;
      int match_columns_string_length;
      qa[n].db = db;
      qa[n].table = table;
      qa[n].res = merge_res ? merge_res : res;
      match_columns_string_length = grn_vector_get_element(ctx, &match_columns_strings, j, &match_columns_string, NULL, NULL);
      qa[n].match_columns_string = match_columns_string;
      qa[n].match_columns_string_length = match_columns_string_length;
      qa[n].query = args[i + 1];
      qa[n].top_n = top_n;
      qa[n].top_n_sort_keys = top_n_sort_keys;
      qa[n].top_n_sort_keys_length = top_n_sort_keys_length;
      qa[n].op = op;
      qa[n].main = (n == 0 && top_n == 0 && op == GRN_OP_OR) ? GRN_TRUE : GRN_FALSE;
      qa[n].m = &m;

      ret = pthread_create(&threads[n], NULL, (void *)thread_query, (void *) &qa[n]);
      if (ret != 0) {
        GRN_PLUGIN_ERROR(ctx, GRN_NO_MEMORY_AVAILABLE,
                         "[parallel_query] failed to create pthread");
        rc = ctx->rc;
        goto exit;
      }
      n++;
      if (n == n_worker || i + QUERY_SET_SIZE >= n_query_args) {
        for (t = 0; t < n; t++) {
          ret = pthread_join(threads[t], NULL);
          if (ret != 0) {
            GRN_PLUGIN_ERROR(ctx, GRN_NO_MEMORY_AVAILABLE,
                             "[parallel_query] failed to join pthread");
            rc = ctx->rc;
            goto exit;
          } else if (rc != GRN_SUCCESS) {
            goto exit;
          }
        }
        n = 0;
      }
    }
#undef QUERY_SET_SIZE
  }

  if (merge_res) {
    grn_obj *sorted;
    if ((sorted = grn_table_create(ctx, NULL, 0, NULL, GRN_OBJ_TABLE_NO_KEY, NULL, merge_res))) {
      uint32_t nkeys;
      grn_table_sort_key *keys;
      if ((keys = grn_table_sort_key_from_str(ctx, top_n_sort_keys, top_n_sort_keys_length, merge_res, &nkeys))) {
        grn_table_sort(ctx, merge_res, 0, top_n, sorted, keys, nkeys);
        rc = grn_sorted_res_add(ctx, sorted, merge_res, res, op);
      }
      grn_table_sort_key_close(ctx, keys, nkeys);
    }
    grn_obj_unlink(ctx, sorted);
  }

  ctx->flags &= ~GRN_CTX_TEMPORARY_DISABLE_II_RESOLVE_SEL_AND;
  grn_ii_resolve_sel_and_(ctx, (grn_hash *)res, op);

exit :
  GRN_OBJ_FIN(ctx, &match_columns_strings);

  pthread_mutex_destroy(&m);

  if (merge_res) {
    grn_obj_unlink(ctx, merge_res);
  }

  return rc;
}

static grn_rc
selector_parallel_query(grn_ctx *ctx, GNUC_UNUSED grn_obj *table, GNUC_UNUSED grn_obj *index,
                        GNUC_UNUSED int nargs, grn_obj **args,
                        grn_obj *res, grn_operator op)
{
  return run_parallel_query(ctx, table, nargs - 1, args + 1, res, op);
}

grn_rc
GRN_PLUGIN_INIT(GNUC_UNUSED grn_ctx *ctx)
{
  {
    char grn_parallel_query_n_worker_env[GRN_ENV_BUFFER_SIZE];

    grn_getenv("GRN_PARALLEL_QUERY_N_WORKER",
               grn_parallel_query_n_worker_env,
               GRN_ENV_BUFFER_SIZE);
    if (grn_parallel_query_n_worker_env[0]) {
      n_worker = atoi(grn_parallel_query_n_worker_env);
    }
  }
  return GRN_SUCCESS;
}

grn_rc
GRN_PLUGIN_REGISTER(grn_ctx *ctx)
{
  {
    grn_obj *selector_proc;

    selector_proc = grn_proc_create(ctx, "parallel_query", -1, GRN_PROC_FUNCTION,
                                    NULL, NULL, NULL, 0, NULL);
    grn_proc_set_selector(ctx, selector_proc, selector_parallel_query);
  }
  return ctx->rc;
}

grn_rc
GRN_PLUGIN_FIN(GNUC_UNUSED grn_ctx *ctx)
{
  return GRN_SUCCESS;
}
