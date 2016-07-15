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

pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;

/* copy from lib/grn_rset.h */
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

typedef struct {
  grn_obj *db;
  grn_obj *table;
  grn_obj *res;
  grn_obj *match_columns_string;
  grn_obj *query;
  grn_operator op;
  grn_bool main;
} thread_query_args;

static void*
thread_query(void *p)
{
  thread_query_args *ip = (thread_query_args *)p;
  grn_ctx ctx_;
  grn_ctx *ctx = &ctx_;
  grn_obj *table = ip->table;
  grn_obj *res = ip->res;
  grn_obj *match_columns_string = ip->match_columns_string;
  grn_obj *query = ip->query;
  grn_operator op = ip->op;
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

  if (match_columns_string->header.domain == GRN_DB_TEXT &&
      GRN_TEXT_LEN(match_columns_string) > 0) {

    GRN_EXPR_CREATE_FOR_QUERY(ctx, table, match_columns, dummy_variable);
    if (!match_columns) {
      rc = ctx->rc;
      goto exit;
    }

    grn_expr_parse(ctx, match_columns,
                   GRN_TEXT_VALUE(match_columns_string),
                   GRN_TEXT_LEN(match_columns_string),
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

    if (ip->main) {
      ret = pthread_mutex_lock(&m);
      if (ret != 0) {
        rc = GRN_NO_LOCKS_AVAILABLE;
        goto exit;
      }
      grn_table_select(ctx, table, condition, res, op);
      rc = ctx->rc;
      ret = pthread_mutex_unlock(&m);
      if (ret != 0) {
        rc = GRN_NO_LOCKS_AVAILABLE;
        goto exit;
      }
    } else {
      thread_res = grn_table_create(ctx, NULL, 0, NULL,
                                    GRN_TABLE_HASH_KEY|GRN_OBJ_WITH_SUBREC,
                                    table, NULL);
      if (!thread_res) {
        rc = ctx->rc;
        goto exit;
      }
      grn_table_select(ctx, table, condition, thread_res, GRN_OP_OR);
      rc = ctx->rc;

      if (grn_table_size(ctx, thread_res) > 0) {
        ret = pthread_mutex_lock(&m);
        if (ret != 0) {
          rc = GRN_NO_LOCKS_AVAILABLE;
          goto exit;
        }
        rc = grn_table_setoperation(ctx, res, thread_res, res, op);
        ret = pthread_mutex_unlock(&m);
        if (ret != 0) {
          rc = GRN_NO_LOCKS_AVAILABLE;
          goto exit;
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
  grn_operator merge_op = GRN_OP_OR;
  grn_bool is_first = GRN_TRUE;
  grn_obj *db = grn_ctx_db(ctx);
  grn_bool use_merge_res = GRN_FALSE;
  grn_obj *merge_res = NULL;
  grn_obj *options;
  grn_bool separate_query = GRN_FALSE;

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
      } else if (key_size == 8 && !memcmp(key, "merge_op", 8)) {
        if (GRN_TEXT_LEN(value) >= 2 &&
            !memcmp(GRN_TEXT_VALUE(value), "OR", 2)) {
          merge_op = GRN_OP_OR;
        } else if (GRN_TEXT_LEN(value) >= 3 &&
                   !memcmp(GRN_TEXT_VALUE(value), "AND", 3)) {
          merge_op = GRN_OP_AND;
        } else if (GRN_TEXT_LEN(value) >= 3 &&
                   !memcmp(GRN_TEXT_VALUE(value), "NOT", 3)) {
          merge_op = GRN_OP_AND_NOT;
        } else if (GRN_TEXT_LEN(value) >= 6 &&
                   !memcmp(GRN_TEXT_VALUE(value), "ADJUST", 6)) {
          merge_op = GRN_OP_ADJUST;
        } else {
          GRN_PLUGIN_ERROR(ctx, GRN_INVALID_ARGUMENT,
                           "invalid option name: <%.*s>",
                           (int)GRN_TEXT_LEN(value), GRN_TEXT_VALUE(value));
          grn_hash_cursor_close(ctx, cursor);
          goto exit;
        }
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

  if ((op == GRN_OP_AND || op == GRN_OP_AND_NOT) &&
      (merge_op == GRN_OP_OR)) {
    use_merge_res = GRN_TRUE;
    merge_res = grn_table_create(ctx, NULL, 0, NULL,
                                 GRN_TABLE_HASH_KEY|GRN_OBJ_WITH_SUBREC,
                                 table, NULL);
    if (!merge_res) {
      rc = ctx->rc;
      goto exit;
    }
  }

  if (!separate_query) {
    for (i = 0; i < n_query_args - 1; i++) {
      qa[n].db = db;
      qa[n].table = table;
      qa[n].res = use_merge_res ? merge_res : res;
      qa[n].match_columns_string = args[i];
      qa[n].query = args[n_query_args - 1];
      if (is_first && !use_merge_res) {
        qa[n].op = op;
        is_first = GRN_FALSE;
      } else {
        qa[n].op = merge_op;
      }
      qa[n].main = n == 0 ? GRN_TRUE : GRN_FALSE;

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
    for (i = 0; i + QUERY_SET_SIZE <= n_query_args; i += QUERY_SET_SIZE) {
      qa[n].db = db;
      qa[n].table = table;
      qa[n].res = use_merge_res ? merge_res : res;
      qa[n].match_columns_string = args[i];
      qa[n].query = args[i + 1];
      if (is_first && !use_merge_res) {
        qa[n].op = op;
        is_first = GRN_FALSE;
      } else {
        qa[n].op = merge_op;
      }
      qa[n].main = n == 0 ? GRN_TRUE : GRN_FALSE;
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

  if (use_merge_res) {
    rc = grn_table_setoperation(ctx, res, merge_res, res, op);
  }

exit :

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
