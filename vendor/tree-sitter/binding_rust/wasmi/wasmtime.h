// Browser runtime adapter for Tree-sitter's existing wasm_store.c.
// All guest code and memory remain inside Wasmi. Handles are owned by their
// store.
#ifndef TREE_SITTER_WASMI_BRIDGE_H
#define TREE_SITTER_WASMI_BRIDGE_H
#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <wasm.h>
#include <wasmi/engine.h>

// Browser Tree-sitter stream functions intentionally have no native I/O.
#define printf(...) fprintf(NULL, __VA_ARGS__)

typedef wasm_trap_t wasmtime_error_t;
extern wasm_trap_t *wasmi_trap_new(const uint8_t *, size_t);
extern wasm_ref_t *ts_wasmi_func_to_ref(const wasm_func_t *);
extern wasm_extern_t *ts_wasmi_ref_to_func(const wasm_store_t *,
                                           const wasm_ref_t *);
extern bool ts_wasmi_store_refuel(wasm_store_t *);
#define wasmtime_trap_new(s, n) wasmi_trap_new((const uint8_t *)(s), (n))
#define wasmtime_error_message wasm_trap_message
#define wasmtime_error_delete wasm_trap_delete
#define wasmtime_engine_clone wasmi_engine_clone
#define wasmtime_module_t wasm_module_t
#define wasmtime_module_delete wasm_module_delete
#define wasmtime_module_imports wasm_module_imports
#define wasmtime_module_exports wasm_module_exports
#define WASMTIME_EXTERN_FUNC WASM_EXTERN_FUNC
#define WASMTIME_EXTERN_GLOBAL WASM_EXTERN_GLOBAL
#define WASMTIME_EXTERN_TABLE WASM_EXTERN_TABLE
#define WASMTIME_EXTERN_MEMORY WASM_EXTERN_MEMORY
#define WASMTIME_FUNCREF WASM_FUNCREF
#define WASMTIME_I32 WASM_I32

typedef struct {
  uintptr_t store_id;
} ts_wasmi_handle;
typedef ts_wasmi_handle wasmtime_func_t;
typedef ts_wasmi_handle wasmtime_global_t;
typedef ts_wasmi_handle wasmtime_table_t;
typedef ts_wasmi_handle wasmtime_memory_t;
typedef ts_wasmi_handle wasmtime_instance_t;
typedef struct {
  uint8_t kind;
  union {
    ts_wasmi_handle func, global, table, memory;
  } of;
} wasmtime_extern_t;
typedef struct {
  uint8_t kind;
  union {
    int32_t i32;
    int64_t i64;
    float f32;
    double f64;
    ts_wasmi_handle funcref;
  } of;
} wasmtime_val_t;
typedef union {
  int32_t i32;
  int64_t i64;
  float f32;
  double f64;
  uint64_t pad[2];
} wasmtime_val_raw_t;
typedef struct ts_wasmi_owned {
  void *value;
  void (*destroy)(void *);
  struct ts_wasmi_owned *next;
} ts_wasmi_owned;
typedef struct {
  wasm_store_t *inner;
  ts_wasmi_owned *owned;
  void *data;
  void (*finalizer)(void *);
} wasmtime_store_t;
typedef wasmtime_store_t wasmtime_context_t;
typedef wasmtime_store_t wasmtime_caller_t;
typedef wasm_trap_t *(*wasmtime_func_unchecked_callback_t)(void *,
                                                           wasmtime_caller_t *,
                                                           wasmtime_val_raw_t *,
                                                           size_t);

static wasmtime_error_t *ts_wasmi_error(const char *text) {
  return wasmtime_trap_new(text, strlen(text));
}
static void ts_wasmi_keep(wasmtime_store_t *store, void *value,
                          void (*destroy)(void *)) {
  ts_wasmi_owned *owned = malloc(sizeof(*owned));
  assert(owned);
  *owned = (ts_wasmi_owned){value, destroy, store->owned};
  store->owned = owned;
}
static void ts_wasmi_extern_delete(void *value) { wasm_extern_delete(value); }
static ts_wasmi_handle ts_wasmi_keep_extern(wasmtime_store_t *store,
                                            wasm_extern_t *value) {
  ts_wasmi_keep(store, value, ts_wasmi_extern_delete);
  return (ts_wasmi_handle){(uintptr_t)value};
}
static wasmtime_store_t *wasmtime_store_new(wasm_engine_t *engine, void *data,
                                            void (*finalizer)(void *)) {
  wasmtime_store_t *store = calloc(1, sizeof(*store));
  assert(store);
  *store = (wasmtime_store_t){
      .inner = wasm_store_new(engine), .data = data, .finalizer = finalizer};
  return store;
}
static void wasmtime_store_delete(wasmtime_store_t *store) {
  while (store->owned) {
    ts_wasmi_owned *owned = store->owned;
    store->owned = owned->next;
    owned->destroy(owned->value);
    free(owned);
  }
  wasm_store_delete(store->inner);
  if (store->finalizer)
    store->finalizer(store->data);
  free(store);
}
static wasmtime_context_t *wasmtime_store_context(wasmtime_store_t *store) {
  return store;
}
static wasmtime_context_t *wasmtime_caller_context(wasmtime_caller_t *caller) {
  return caller;
}
static wasmtime_error_t *wasmtime_module_new(wasm_engine_t *engine,
                                             const uint8_t *bytes, size_t size,
                                             wasm_module_t **out) {
  if (size > 64 * 1024 * 1024)
    return ts_wasmi_error("Grammar module exceeds 64 MiB");
  wasm_store_t *store = wasm_store_new(engine);
  wasm_byte_vec_t data = {size, (wasm_byte_t *)bytes};
  *out = wasm_module_new(store, &data);
  wasm_store_delete(store);
  return *out ? NULL
              : ts_wasmi_error("Wasmi rejected the syntax grammar module");
}
static wasm_extern_t *ts_wasmi_extern(ts_wasmi_handle handle) {
  return (wasm_extern_t *)handle.store_id;
}
static wasm_func_t *ts_wasmi_func(const wasmtime_func_t *func) {
  return wasm_extern_as_func(ts_wasmi_extern(*func));
}
static wasm_val_t ts_wasmi_value(const wasmtime_val_t *value) {
  wasm_val_t out = {.kind = value->kind};
  if (value->kind == WASM_FUNCREF)
    out.of.ref = value->of.funcref.store_id
                     ? ts_wasmi_func_to_ref(ts_wasmi_func(&value->of.funcref))
                     : NULL;
  else
    memcpy(&out.of, &value->of, sizeof(value->of.i64));
  return out;
}
static void ts_wasmi_result(wasmtime_val_t *out, const wasm_val_t *value) {
  out->kind = value->kind;
  assert(value->kind != WASM_FUNCREF);
  memcpy(&out->of, &value->of, sizeof(value->of.i64));
}
static wasmtime_error_t *wasmtime_global_new(wasmtime_context_t *store,
                                             const wasm_globaltype_t *type,
                                             const wasmtime_val_t *value,
                                             wasmtime_global_t *out) {
  wasm_val_t val = ts_wasmi_value(value);
  wasm_global_t *global = wasm_global_new(store->inner, type, &val);
  if (!global)
    return ts_wasmi_error("Could not allocate grammar global");
  *out = ts_wasmi_keep_extern(store, wasm_global_as_extern(global));
  return NULL;
}
static uint64_t wasmtime_memorytype_minimum(const wasm_memorytype_t *type) {
  return wasm_memorytype_limits(type)->min;
}
static wasmtime_error_t *wasmtime_memory_new(wasmtime_context_t *store,
                                             const wasm_memorytype_t *type,
                                             wasmtime_memory_t *out) {
  wasm_limits_t limits = *wasm_memorytype_limits(type);
  if (limits.min > 2048)
    return ts_wasmi_error("Grammar memory exceeds 128 MiB");
  if (limits.max > 2048)
    limits.max = 2048;
  wasm_memorytype_t *bounded = wasm_memorytype_new(&limits);
  wasm_memory_t *memory = wasm_memory_new(store->inner, bounded);
  wasm_memorytype_delete(bounded);
  if (!memory)
    return ts_wasmi_error("Could not allocate grammar memory");
  *out = ts_wasmi_keep_extern(store, wasm_memory_as_extern(memory));
  return NULL;
}
static uint8_t *wasmtime_memory_data(wasmtime_context_t *store,
                                     const wasmtime_memory_t *memory) {
  return (uint8_t *)wasm_memory_data(
      wasm_extern_as_memory(ts_wasmi_extern(*memory)));
}
static size_t wasmtime_memory_data_size(wasmtime_context_t *store,
                                        const wasmtime_memory_t *memory) {
  return wasm_memory_data_size(wasm_extern_as_memory(ts_wasmi_extern(*memory)));
}
static wasmtime_error_t *wasmtime_memory_grow(wasmtime_context_t *store,
                                              const wasmtime_memory_t *memory,
                                              uint64_t delta,
                                              uint64_t *previous) {
  wasm_memory_t *inner = wasm_extern_as_memory(ts_wasmi_extern(*memory));
  *previous = wasm_memory_size(inner);
  return delta <= UINT32_MAX && wasm_memory_grow(inner, delta)
             ? NULL
             : ts_wasmi_error("Grammar memory limit exceeded");
}
static wasmtime_error_t *wasmtime_table_new(wasmtime_context_t *store,
                                            const wasm_tabletype_t *type,
                                            const wasmtime_val_t *value,
                                            wasmtime_table_t *out) {
  wasm_limits_t limits = *wasm_tabletype_limits(type);
  if (limits.min > 100000)
    return ts_wasmi_error("Grammar function table exceeds 100000 entries");
  if (limits.max > 100000)
    limits.max = 100000;
  wasm_tabletype_t *bounded =
      wasm_tabletype_new(wasm_valtype_new(WASM_FUNCREF), &limits);
  wasm_val_t val = ts_wasmi_value(value);
  wasm_table_t *table = wasm_table_new(store->inner, bounded, val.of.ref);
  wasm_val_delete(&val);
  wasm_tabletype_delete(bounded);
  if (!table)
    return ts_wasmi_error("Could not allocate grammar function table");
  *out = ts_wasmi_keep_extern(store, wasm_table_as_extern(table));
  return NULL;
}
static wasmtime_error_t *wasmtime_table_grow(wasmtime_context_t *store,
                                             const wasmtime_table_t *table,
                                             uint32_t delta,
                                             const wasmtime_val_t *value,
                                             uint64_t *previous) {
  wasm_table_t *inner = wasm_extern_as_table(ts_wasmi_extern(*table));
  *previous = wasm_table_size(inner);
  wasm_val_t val = ts_wasmi_value(value);
  bool ok = delta <= 100000 && *previous <= 100000 - delta &&
            wasm_table_grow(inner, delta, val.of.ref);
  wasm_val_delete(&val);
  return ok ? NULL : ts_wasmi_error("Grammar table growth failed");
}
static wasmtime_error_t *wasmtime_table_set(wasmtime_context_t *store,
                                            const wasmtime_table_t *table,
                                            uint32_t index,
                                            const wasmtime_val_t *value) {
  wasm_val_t val = ts_wasmi_value(value);
  bool ok = wasm_table_set(wasm_extern_as_table(ts_wasmi_extern(*table)), index,
                           val.of.ref);
  wasm_val_delete(&val);
  return ok ? NULL : ts_wasmi_error("Invalid grammar table index");
}
static bool wasmtime_table_get(wasmtime_context_t *store,
                               const wasmtime_table_t *table, uint32_t index,
                               wasmtime_val_t *out) {
  wasm_table_t *inner = wasm_extern_as_table(ts_wasmi_extern(*table));
  if (index >= wasm_table_size(inner))
    return false;
  wasm_ref_t *ref = wasm_table_get(inner, index);
  *out = (wasmtime_val_t){.kind = WASM_FUNCREF};
  if (ref) {
    wasm_extern_t *func = ts_wasmi_ref_to_func(store->inner, ref);
    wasm_ref_delete(ref);
    if (!func)
      return false;
    out->of.funcref = ts_wasmi_keep_extern(store, func);
  }
  return true;
}
typedef struct {
  wasm_instance_t *inner;
  wasm_extern_vec_t exports;
  wasm_exporttype_vec_t types;
} ts_wasmi_instance;
static void ts_wasmi_instance_delete(void *value) {
  ts_wasmi_instance *instance = value;
  wasm_extern_vec_delete(&instance->exports);
  wasm_exporttype_vec_delete(&instance->types);
  wasm_instance_delete(instance->inner);
  free(instance);
}
static wasmtime_error_t *
wasmtime_instance_new(wasmtime_context_t *store, const wasm_module_t *module,
                      const wasmtime_extern_t *imports, size_t count,
                      wasmtime_instance_t *out, wasm_trap_t **trap) {
  if (!ts_wasmi_store_refuel(store->inner))
    return ts_wasmi_error("Grammar engine must enable instruction fuel");
  wasm_extern_t **items = calloc(count ? count : 1, sizeof(*items));
  assert(items);
  for (size_t i = 0; i < count; i++)
    items[i] = ts_wasmi_extern(imports[i].of.func);
  wasm_extern_vec_t list = {count, items};
  wasm_instance_t *inner = wasm_instance_new(store->inner, module, &list, trap);
  free(items);
  if (!inner)
    return *trap ? NULL : ts_wasmi_error("Grammar instantiation failed");
  ts_wasmi_instance *instance = calloc(1, sizeof(*instance));
  assert(instance);
  instance->inner = inner;
  wasm_instance_exports(inner, &instance->exports);
  wasm_module_exports(module, &instance->types);
  assert(instance->exports.size == instance->types.size);
  ts_wasmi_keep(store, instance, ts_wasmi_instance_delete);
  out->store_id = (uintptr_t)instance;
  return NULL;
}
static bool wasmtime_instance_export_nth(wasmtime_context_t *store,
                                         const wasmtime_instance_t *instance,
                                         size_t index, char **name,
                                         size_t *name_length,
                                         wasmtime_extern_t *out) {
  ts_wasmi_instance *inner = (ts_wasmi_instance *)instance->store_id;
  if (index >= inner->exports.size)
    return false;
  const wasm_name_t *export_name =
      wasm_exporttype_name(inner->types.data[index]);
  *name = export_name->data;
  *name_length = export_name->size;
  wasm_extern_t *value = inner->exports.data[index];
  out->kind = wasm_extern_kind(value);
  out->of.func.store_id = (uintptr_t)value;
  return true;
}
typedef struct {
  wasmtime_context_t *store;
  wasmtime_func_unchecked_callback_t call;
  void *data;
  void (*destroy)(void *);
} ts_wasmi_host;
static void ts_wasmi_host_delete(void *value) {
  ts_wasmi_host *host = value;
  if (host->destroy)
    host->destroy(host->data);
  free(host);
}
static wasm_trap_t *ts_wasmi_host_call(void *data, const wasm_val_vec_t *args,
                                       wasm_val_vec_t *results) {
  ts_wasmi_host *host = data;
  size_t count = args->size > results->size ? args->size : results->size;
  if (count > 16)
    return ts_wasmi_error("Unsupported grammar host function arity");
  wasmtime_val_raw_t values[16] = {0};
  for (size_t i = 0; i < args->size; i++) {
    if (args->data[i].kind != WASM_I32)
      return ts_wasmi_error("Unsupported grammar host argument");
    values[i].i32 = args->data[i].of.i32;
  }
  wasm_trap_t *trap = host->call(host->data, host->store, values, count);
  if (!trap)
    for (size_t i = 0; i < results->size; i++)
      results->data[i] = (wasm_val_t)WASM_I32_VAL(values[i].i32);
  return trap;
}
static void wasmtime_func_new_unchecked(
    wasmtime_context_t *store, const wasm_functype_t *type,
    wasmtime_func_unchecked_callback_t callback, void *data,
    void (*finalizer)(void *), wasmtime_func_t *out) {
  ts_wasmi_host *host = malloc(sizeof(*host));
  assert(host);
  *host = (ts_wasmi_host){store, callback, data, finalizer};
  wasm_func_t *func = wasm_func_new_with_env(
      store->inner, type, ts_wasmi_host_call, host, ts_wasmi_host_delete);
  *out = ts_wasmi_keep_extern(store, wasm_func_as_extern(func));
}
static wasmtime_error_t *
wasmtime_func_call(wasmtime_context_t *store, const wasmtime_func_t *func,
                   const wasmtime_val_t *args, size_t args_length,
                   wasmtime_val_t *results, size_t results_length,
                   wasm_trap_t **trap) {
  if (!ts_wasmi_store_refuel(store->inner))
    return ts_wasmi_error("Grammar engine must enable instruction fuel");
  if (args_length > 16 || results_length > 16)
    return ts_wasmi_error("Unsupported grammar call arity");
  wasm_val_t in[16] = {0}, out[16] = {0};
  for (size_t i = 0; i < args_length; i++)
    in[i] = ts_wasmi_value(&args[i]);
  wasm_val_vec_t inputs = {args_length, in}, outputs = {results_length, out};
  *trap = wasm_func_call(ts_wasmi_func(func), &inputs, &outputs);
  if (!*trap)
    for (size_t i = 0; i < results_length; i++)
      ts_wasmi_result(&results[i], &out[i]);
  return NULL;
}
static wasmtime_error_t *wasmtime_func_call_unchecked(
    wasmtime_context_t *store, const wasmtime_func_t *func,
    wasmtime_val_raw_t *values, size_t capacity, wasm_trap_t **trap) {
  if (!ts_wasmi_store_refuel(store->inner))
    return ts_wasmi_error("Grammar engine must enable instruction fuel");
  wasm_functype_t *type = wasm_func_type(ts_wasmi_func(func));
  size_t ins = wasm_functype_params(type)->size,
         outs = wasm_functype_results(type)->size;
  if (ins > capacity || outs > capacity || ins > 16 || outs > 16) {
    wasm_functype_delete(type);
    return ts_wasmi_error("Invalid grammar call signature");
  }
  for (size_t i = 0; i < outs; i++)
    if (wasm_valtype_kind(wasm_functype_results(type)->data[i]) != WASM_I32) {
      wasm_functype_delete(type);
      return ts_wasmi_error("Unsupported grammar result type");
    }
  wasm_val_t args[16] = {0}, results[16] = {0};
  for (size_t i = 0; i < ins; i++) {
    if (wasm_valtype_kind(wasm_functype_params(type)->data[i]) != WASM_I32) {
      wasm_functype_delete(type);
      return ts_wasmi_error("Unsupported grammar argument type");
    }
    args[i] = (wasm_val_t)WASM_I32_VAL(values[i].i32);
  }
  wasm_functype_delete(type);
  wasm_val_vec_t in = {ins, args}, out = {outs, results};
  *trap = wasm_func_call(ts_wasmi_func(func), &in, &out);
  if (!*trap)
    for (size_t i = 0; i < outs; i++) {
      assert(results[i].kind == WASM_I32);
      values[i].i32 = results[i].of.i32;
    }
  return NULL;
}
#endif
