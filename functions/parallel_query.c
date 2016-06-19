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
  const char *match_columns_string;
  unsigned int match_columns_string_length;
  const char *query_string;
  unsigned int query_string_length;
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
  const char *match_columns_string = ip->match_columns_string;
  unsigned int match_columns_string_length = ip->match_columns_string_length;
  const char *query_string = ip->query_string;
  unsigned int query_string_length = ip->query_string_length;
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

  if (query_string_length > 0) {
    grn_expr_flags flags =
      GRN_EXPR_SYNTAX_QUERY|GRN_EXPR_ALLOW_PRAGMA|GRN_EXPR_ALLOW_COLUMN;

    GRN_EXPR_CREATE_FOR_QUERY(ctx, table, condition, dummy_variable);
    if (!condition) {
      rc = ctx->rc;
      goto exit;
    }

    grn_expr_parse(ctx, condition,
                   query_string,
                   query_string_length,
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
  grn_obj queries;
  GRN_TEXT_INIT(&queries, GRN_OBJ_VECTOR);

  if (nargs < 2) {
    GRN_PLUGIN_ERROR(ctx, GRN_INVALID_ARGUMENT,
                     "wrong number of arguments (%d for 2..)", nargs);
    rc = ctx->rc;
    goto exit;
  }
#define QUERY_SET_SIZE 2
  if (nargs > 2 && nargs % QUERY_SET_SIZE) {
    if (GRN_TEXT_LEN(args[nargs - 1]) >= 2 &&
        !memcmp(GRN_TEXT_VALUE(args[nargs - 1]), "OR", 2)) {
      merge_op = GRN_OP_OR;
    } else if (GRN_TEXT_LEN(args[nargs - 1]) >= 3 &&
               !memcmp(GRN_TEXT_VALUE(args[nargs - 1]), "AND", 3)) {
      merge_op = GRN_OP_AND;
    } else if (GRN_TEXT_LEN(args[nargs - 1]) >= 3 &&
               !memcmp(GRN_TEXT_VALUE(args[nargs - 1]), "NOT", 3)) {
      merge_op = GRN_OP_AND_NOT;
    } else if (GRN_TEXT_LEN(args[nargs - 1]) >= 6 &&
               !memcmp(GRN_TEXT_VALUE(args[nargs - 1]), "ADJUST", 6)) {
      merge_op = GRN_OP_ADJUST;
    }
    n_query_args--;
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

  for (i = 0; i + QUERY_SET_SIZE <= n_query_args; i += QUERY_SET_SIZE) {
    if ((args[i]->header.domain == GRN_DB_TEXT && GRN_TEXT_LEN(args[i]) > 0) &&
        (args[i + 1]->header.domain == GRN_DB_TEXT && GRN_TEXT_LEN(args[i + 1]) > 0)) {
#define ADD(match_columns_, match_columns_length_, query_, query_length_)   \
  grn_vector_add_element(ctx, &queries,                                     \
                         match_columns_, match_columns_length_,             \
                         0, GRN_DB_TEXT);                                   \
  grn_vector_add_element(ctx, &queries,                                     \
                         query_, query_length_,                             \
                         0, GRN_DB_TEXT)

      const char *s = GRN_TEXT_VALUE(args[i]);
      const char *e = GRN_BULK_CURR(args[i]);
      const char *p = s, *last_pipe = s;
      unsigned int cl = 0;
      grn_bool have_pipe = GRN_FALSE;
      if (e - p >= 1 && !memcmp(p, "*", 1)) {
        p++;
        ADD(p, e - p, GRN_TEXT_VALUE(args[i + 1]), GRN_TEXT_LEN(args[i + 1]));
      } else {
        for (p = s; p < e && (cl = grn_charlen(ctx, p, e)); p += cl) {
          if (e - p >= 2 && !memcmp(p, "||", 2)) {
            ADD(last_pipe, p - last_pipe, GRN_TEXT_VALUE(args[i + 1]), GRN_TEXT_LEN(args[i + 1]));
            last_pipe = p + 2;
            have_pipe = GRN_TRUE;
          }
        }
        if (have_pipe) {
          ADD(last_pipe, e - last_pipe, GRN_TEXT_VALUE(args[i + 1]), GRN_TEXT_LEN(args[i + 1]));
        } else {
          ADD(GRN_TEXT_VALUE(args[i]), GRN_TEXT_LEN(args[i]),
              GRN_TEXT_VALUE(args[i + 1]), GRN_TEXT_LEN(args[i + 1]));
        }
      }
    }
  }

  n_query_args = grn_vector_size(ctx, &queries);

  for (i = 0; i + QUERY_SET_SIZE <= n_query_args; i += QUERY_SET_SIZE) {
    qa[n].db = db;
    qa[n].table = table;
    qa[n].res = use_merge_res ? merge_res : res;
    qa[n].match_columns_string_length =
      grn_vector_get_element(ctx, &queries, i,
                              &(qa[n].match_columns_string), NULL, NULL);
    qa[n].query_string_length =
      grn_vector_get_element(ctx, &queries, i + 1,
                              &(qa[n].query_string), NULL, NULL);

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

  if (use_merge_res) {
    rc = grn_table_setoperation(ctx, res, merge_res, res, op);
  }

#undef QUERY_SET_SIZE

exit :

  GRN_OBJ_FIN(ctx, &queries);

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
