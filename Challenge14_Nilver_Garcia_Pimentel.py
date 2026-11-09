#!/usr/bin/env python3
"""Challenge 14: Concurrent Bank Transfers with Deadlock Prevention.

Un solo archivo ejecutable con Python 3.9+ (standard library).
Los conceptos técnicos se conservan en inglés; explicación y consola en español.

Ejemplos:
    python Challenge14_Nilver_Garcia_Pimentel.py --help
    python Challenge14_Nilver_Garcia_Pimentel.py --accounts 10 --balance 1000 --transfers 100 --max-amount 100
    python Challenge14_Nilver_Garcia_Pimentel.py --experiments --repetitions 5
"""
import argparse
import os
import random
import statistics
import threading
import time
from dataclasses import dataclass, field


@dataclass
class Account:
    uid: int
    balance: int
    lock: object = field(default_factory=threading.Lock, repr=False)


@dataclass(frozen=True)
class TransferTask:
    uid: int
    source: int
    target: int
    amount: int


@dataclass
class Result:
    status: str = "NO_INICIADA"
    wait_ns: int = 0
    error: str = ""


def create_workload(accounts, operations, max_amount, seed):
    """Construye una carga determinista sin transferencias hacia la misma cuenta."""
    rng = random.Random(seed)
    tasks = []
    for uid in range(operations):
        source = rng.randrange(accounts)
        target = (source + rng.randrange(1, accounts)) % accounts
        tasks.append(TransferTask(uid, source, target, rng.randint(1, max_amount)))
    return tasks


def serial_reference(num_accounts, initial_balance, tasks):
    """Mismo trabajo lógico y orden de tareas, sin Thread ni Locks."""
    balances = [initial_balance] * num_accounts
    states = []
    for task in tasks:
        if balances[task.source] < task.amount:
            states.append("FONDOS_INSUFICIENTES")
            continue
        balances[task.source] -= task.amount
        balances[task.target] += task.amount
        states.append("OK")
    return balances, states


def parallel_transfer(accounts, task, history, history_lock):
    """Global Lock Ordering elimina Circular Wait; sin sleep artificiales."""
    source = accounts[task.source]
    target = accounts[task.target]
    first, second = sorted((source, target), key=lambda account: account.uid)
    since = time.perf_counter_ns()
    with first.lock:
        with second.lock:
            waited = time.perf_counter_ns() - since
            if source.balance < task.amount:
                status = "FONDOS_INSUFICIENTES"
            else:
                source.balance -= task.amount
                target.balance += task.amount
                status = "OK"
            # El historial se ordena DENTRO de ambos Locks de cuentas.
            # history_lock protege solo el registro (no todo el movimiento bancario).
            # El replay Serial verifica el mismo trabajo en el orden observable.
            with history_lock:
                history.append((task.uid, status))
            return status, waited


def serial_replay(num_accounts, initial_balance, tasks, history):
    """Referencia Serial en el orden real de linearización de Parallel.

    El historial lo registra cada Thread dentro de los Locks de ambas cuentas.
    Esto valida TODAS las transacciones, también los rechazos, sin alterar saldos.
    No equivale a exigir que la ejecución concurrente respete el orden de uid.
    """
    balances = [initial_balance] * num_accounts
    expected = []
    for uid, status in history:
        task = tasks[uid]
        if balances[task.source] >= task.amount:
            balances[task.source] -= task.amount
            balances[task.target] += task.amount
            expected.append((uid, "OK"))
        else:
            expected.append((uid, "FONDOS_INSUFICIENTES"))
    return balances, expected


def run_case(num_accounts, initial_balance, operations, max_amount, seed,
             *, logs=False, title="Prueba", require_exact=False):
    tasks = create_workload(num_accounts, operations, max_amount, seed)
    accounts = [Account(uid, initial_balance) for uid in range(num_accounts)]
    results = [Result() for _ in tasks]
    start_event = threading.Event()
    history_lock = threading.Lock()
    history = []
    workers = []
    start_error = None

    def worker(task):
        try:
            start_event.wait()
            status, waited = parallel_transfer(accounts, task, history, history_lock)
            results[task.uid] = Result(status, waited)
        except Exception as exc:
            results[task.uid] = Result("ERROR", error=repr(exc))

    started_at = time.perf_counter()
    try:
        for task in tasks:
            thread = threading.Thread(target=worker, args=(task,), name=f"T{task.uid:05d}")
            thread.start()
            workers.append(thread)
    except RuntimeError as exc:
        start_error = repr(exc)
    finally:
        start_event.set()
        for thread in workers:
            thread.join()
    elapsed = time.perf_counter() - started_at

    balances = [account.balance for account in accounts]
    reconstructed = [initial_balance] * num_accounts
    statuses = [result.status for result in results]
    for task, result in zip(tasks, results):
        if result.status == "OK":
            reconstructed[task.source] -= task.amount
            reconstructed[task.target] += task.amount

    counts = {state: statuses.count(state) for state in
              ("OK", "FONDOS_INSUFICIENTES", "ERROR", "NO_INICIADA")}
    # Referencia Serial independiente en orden de uid, informativa: si hay rechazos,
    # diferentes órdenes legales pueden dar resultados distintos.
    serial_balances, serial_states = serial_reference(num_accounts, initial_balance, tasks)
    fixed_order_match = balances == serial_balances and statuses == serial_states

    # Verificación fuerte de linearizabilidad: misma carga y mismos saldos iniciales,
    # TODOS los intentos reproducidos Serial según el orden real de decisiones.
    replay_balances, replay_states = serial_replay(num_accounts, initial_balance, tasks, history)
    history_complete = (len(history) == operations and
                        sorted(uid for uid, _ in history) == list(range(operations)))
    replay_equal = (history_complete and balances == replay_balances and
                    list(history) == replay_states and
                    all(statuses[uid] == state for uid, state in history))

    checks = {
        "conservacion_dinero": sum(balances) == num_accounts * initial_balance,
        "saldos_no_negativos": all(amount >= 0 for amount in balances),
        "auditoria_por_cuenta": reconstructed == balances,
        "todos_los_threads_finalizaron": len(workers) == operations and
        all(not thread.is_alive() for thread in workers),
        "operaciones_contabilizadas": counts["OK"] + counts["FONDOS_INSUFICIENTES"] == operations,
        "sin_excepciones": start_error is None and counts["ERROR"] == 0,
        "historial_completo": history_complete,
        "serial_parallel_igualdad": replay_equal,
    }

    wait_times = [result.wait_ns / 1e6 for result in results
                  if result.status in ("OK", "FONDOS_INSUFICIENTES")]
    cores = os.cpu_count() or 1
    print(f"\n=== {title} ===")
    print(f"cores_detectados={cores} threads_creados={len(workers)} "
          f"operaciones={operations} cuentas={num_accounts} balance={initial_balance} "
          f"max_amount={max_amount} seed={seed}")
    print("nota_cores: os.cpu_count() detecta CPUs lógicas, no mide su utilización; "
          "el GIL puede limitar el paralelismo CPU-bound")
    if logs:
        for task, result in zip(tasks, results):
            print(f"T{task.uid:05d} origen=A{task.source} destino=A{task.target} "
                  f"monto={task.amount} estado={result.status} "
                  f"lock_wait_ms={result.wait_ns / 1e6:.6f}" +
                  (f" error={result.error}" if result.error else ""))
    print(f"transferencias: realizadas={counts['OK']} "
          f"rechazadas={counts['FONDOS_INSUFICIENTES']} "
          f"errores={counts['ERROR']} no_iniciadas={counts['NO_INICIADA']}")
    print(f"dinero_inicial={num_accounts * initial_balance} dinero_final={sum(balances)}")
    print(f"tiempo_s={elapsed:.6f} throughput_intentos_s={operations / elapsed:.2f}")
    print(f"lock_wait_mediana_ms={statistics.median(wait_times) if wait_times else 0:.6f} "
          f"lock_wait_max_ms={max(wait_times, default=0):.6f}")
    print("referencia_serial: mismo workload, mismo saldo inicial, "
          "replay de todos los intentos en orden de linearizacion")
    print(f"serial_orden_uid: saldos_y_estados_iguales={'SI' if fixed_order_match else 'NO'} "
          "(informativo: el orden concurrente puede variar con fondos insuficientes)")
    for name, passed in checks.items():
        print(f"{name}: {'PASS' if passed else 'FAIL'}")
    if start_error:
        print(f"error_creacion_threads={start_error}")
    print("deadlock_prevention: Global Lock Ordering (uid ascendente), sin Circular Wait")
    passed = all(checks.values())
    print(f"RESULTADO: {'PASS' if passed else 'FAIL'}")
    return passed, elapsed, operations / elapsed


def experiments(args):
    cores = os.cpu_count() or 1
    # El enunciado pide variar cuentas y Threads; se controla saldo inicial,
    # max_amount y semilla, SIN alterar balance entre escenarios.
    small_threads = max(40, cores)
    large_threads = max(120, 2 * small_threads)
    scenarios = [("Low Contention", max(100, cores), small_threads),
                 ("High Contention", 2, small_threads),
                 ("Low Contention", max(100, cores), large_threads),
                 ("High Contention", 2, large_threads)]
    print(f"CONTROL_EXPERIMENTAL: balance_fijo={args.balance} "
          f"max_amount_fijo={args.max_amount} seeds={args.seed}.."
          f"{args.seed + args.repetitions - 1}; "
          "Serial replay valida incluso transferencias rechazadas")
    everything_ok = True
    for label, num_accounts, operations in scenarios:
        times, throughputs = [], []
        for repetition in range(args.repetitions):
            seed = args.seed + repetition
            ok, elapsed, throughput = run_case(
                num_accounts, args.balance, operations, args.max_amount, seed,
                logs=args.logs, title=f"{label}: {num_accounts} cuentas, "
                f"{operations} threads, corrida {repetition + 1}")
            everything_ok = everything_ok and ok
            times.append(elapsed)
            throughputs.append(throughput)
        print(f"RESUMEN {label} cuentas={num_accounts} threads={operations} "
              f"repeticiones={args.repetitions} mediana_s={statistics.median(times):.6f} "
              f"min_s={min(times):.6f} max_s={max(times):.6f} "
              f"throughput_mediano={statistics.median(throughputs):.2f}")
    print("ANALISIS: más Threads no implican mayor Throughput. Los tiempos incluyen "
          "Thread Creation Overhead y registro de auditoria; GIL y Thread Scheduling "
          "afectan las mediciones. Lock Ordering previene Deadlock, pero no "
          "garantiza Fairness ni Starvation-freedom.")
    print(f"RESULTADO GLOBAL EXPERIMENTOS: {'PASS' if everything_ok else 'FAIL'}")
    return 0 if everything_ok else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--accounts", type=int, default=10, help="número de cuentas (>=2)")
    parser.add_argument("--balance", type=int, default=1000, help="saldo inicial de cada cuenta (>=0)")
    parser.add_argument("--transfers", type=int, default=max(2, os.cpu_count() or 2),
                        help="operaciones/threads; por defecto, CPUs lógicas detectadas")
    parser.add_argument("--max-amount", type=int, default=100, help="monto máximo (>0)")
    parser.add_argument("--seed", type=int, default=2026, help="semilla reproducible")
    parser.add_argument("--logs", action="store_true", help="imprimir log de cada transferencia")
    parser.add_argument("--experiments", action="store_true", help="Low/High Contention con referencia Serial")
    parser.add_argument("--repetitions", type=int, default=5, help="corridas por escenario (>=1)")
    args = parser.parse_args()
    if (args.accounts < 2 or args.balance < 0 or args.transfers < 1
            or args.max_amount < 1 or args.repetitions < 1):
        parser.error("Se requiere accounts>=2, balance>=0, transfers>=1, "
                     "max-amount>=1 y repetitions>=1")
    if args.experiments:
        return experiments(args)
    # En todas las pruebas se respeta exactamente --balance; --transfers
    # determina los intentos en modo personalizado.
    passed, _, _ = run_case(args.accounts, args.balance, args.transfers,
                            args.max_amount, args.seed, logs=args.logs,
                            title="Escenario personalizado")
    return 0 if passed else 1


# REFLECTION QUESTIONS (respuestas exigidas como comentarios en el código)
# 1. Las cuatro condiciones de Coffman son Mutual Exclusion, Hold and Wait,
#    No Preemption y Circular Wait. Deben coexistir para producir Deadlock.
# 2. Se usa Global Lock Ordering: dos cuentas se bloquean por uid ascendente,
#    aunque la transferencia se haga en sentido inverso. Esto elimina Circular Wait.
# 3. Un único Global Lock mantiene Correctness pero impide transferencias
#    independientes simultáneas, aumentando innecesariamente Contention.
# 4. Más Threads pueden aumentar Context Switching, Thread Scheduling Overhead
#    y Lock Contention. En CPython tradicional el GIL limita trabajo CPU-bound;
#    crear Threads no demuestra utilización de todos los cores.
# 5. Para más de dos cuentas se ordenarían TODOS sus Lock por uid, se validarían
#    las condiciones antes de modificar saldos y se liberarían con context managers.
#    Para persistencia real harían falta rollback y Atomic Transactions.
#
# CONDICIONES TRANSVERSALES DE LOS RETOS:
# - Cores: detectados con os.cpu_count(); el modo default y experimentos
#   crean al menos esa cantidad de Threads (salvo configuración explícita).
# - Serial/Parallel: mismas operaciones, mismos saldos y montos. El replay
#   Serial verifica la historia real de decisiones (linearizabilidad), incluso
#   con fondos insuficientes. El orden fijo de uid es comparación informativa
#   porque no siempre coincide con el orden concurrente observable.
# - Sin retrasos artificiales: NO se utiliza sleep().
# - Igualdad de resultados: saldo de cada cuenta y estado de cada tarea se
#   contrastan con el replay Serial en TODAS las ejecuciones; ninguna prueba
#   altera el saldo inicial especificado por el usuario.
#
# REFERENCIAS: Raynal (2013), Concurrent Programming: Algorithms, Principles,
# and Foundations. Python threading: https://docs.python.org/3/library/threading.html


if __name__ == "__main__":
    raise SystemExit(main())
