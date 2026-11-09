#!/usr/bin/env python3
"""UNIVERSIDAD TECNOLÓGICA DE BOLÍVAR (UTB)
Curso: Computación en Paralelo (ISCO-A08A)
Docente: Juan Carlos Martínez
Estudiante: Nilver García Pimentel
Reto: Challenge 14 — Concurrent Bank Transfers with Deadlock Prevention

OBJETIVO
Simular transferencias bancarias concurrentes con Thread y Lock por cuenta,
garantizando la conservación del dinero y previniendo Deadlock mediante
Global Lock Ordering (orden ascendente de identificador de cuenta).

EJECUCIÓN
Este archivo .txt contiene código Python autónomo (Python 3.9+, standard library).
Se ejecuta directamente desde una terminal; no requiere otros archivos.

Ejemplos:
  python Challenge14_Nilver_Garcia_Pimentel.txt --help
  python Challenge14_Nilver_Garcia_Pimentel.txt --accounts 10 --balance 1000 --transfers 100 --max-amount 100
  python Challenge14_Nilver_Garcia_Pimentel.txt --accounts 2 --balance 0 --transfers 16 --logs
  python Challenge14_Nilver_Garcia_Pimentel.txt --experiments --repetitions 5

NOTA DE INGENIERÍA
Los Threads no garantizan ocupar todos los cores físicos. La referencia
Serial Replay valida el historial realmente ejecutado (linearizabilidad);
con fondos insuficientes, un orden Serial fijo puede diferir del concurrente.
Las respuestas a las cinco preguntas de reflexión están al final del código.
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
    width = 68
    print("\n" + "=" * width)
    print("UTB | COMPUTACIÓN EN PARALELO | CHALLENGE 14")
    print("Concurrent Bank Transfers with Deadlock Prevention")
    print(f"Escenario: {title}")
    print("=" * width)

    print("\n[1] CONFIGURACIÓN")
    print(f"  Cuentas:                         {num_accounts}")
    print(f"  Saldo inicial por cuenta:        {initial_balance}")
    print(f"  Transferencias solicitadas:     {operations}")
    print(f"  Threads creados:                {len(workers)}")
    print(f"  CPUs lógicas detectadas:        {cores}")
    print(f"  Monto máximo por transferencia: {max_amount}")
    print(f"  Semilla (seed):                 {seed}")
    print("  Nota: el número de Threads no acredita uso simultáneo de cores; "
          "el GIL puede limitar trabajo CPU-bound.")

    print("\n[2] EJECUCIÓN DE TRANSFERENCIAS")
    if logs:
        print("  Log individual de operaciones:")
        for task, result in zip(tasks, results):
            print(f"  T{task.uid:05d} origen=A{task.source} destino=A{task.target} "
                  f"monto={task.amount} estado={result.status} "
                  f"lock_wait_ms={result.wait_ns / 1e6:.6f}" +
                  (f" error={result.error}" if result.error else ""))
    else:
        print("  Logs individuales: ocultos (usar --logs para mostrarlos)")
    print(f"  Intentadas:                     {operations}")
    print(f"  Exitosas:                       {counts['OK']}")
    print(f"  Rechazadas por fondos:          {counts['FONDOS_INSUFICIENTES']}")
    print(f"  Errores:                        {counts['ERROR']}")
    print(f"  No iniciadas:                   {counts['NO_INICIADA']}")

    print("\n[3] INTEGRIDAD BANCARIA")
    print(f"  Dinero inicial:                 {num_accounts * initial_balance}")
    print(f"  Dinero final:                   {sum(balances)}")
    for name in ("conservacion_dinero", "saldos_no_negativos", "auditoria_por_cuenta",
                 "todos_los_threads_finalizaron", "operaciones_contabilizadas", "sin_excepciones"):
        print(f"  {name}: {'PASS' if checks[name] else 'FAIL'}")

    print("\n[4] SINCRONIZACIÓN Y DEADLOCK PREVENTION")
    print("  Estrategia: Global Lock Ordering (uid ascendente)")
    print("  Locks: uno independiente por cuenta; Circular Wait prevenido por diseño")
    print(f"  historial_completo: {'PASS' if checks['historial_completo'] else 'FAIL'}")

    print("\n[5] VALIDACIÓN SERIAL / PARALLEL")
    print("  Referencia: Serial Replay de los mismos intentos, montos y saldo inicial")
    print("  Orden: historial de linearización de la ejecución Parallel")
    print(f"  serial_parallel_igualdad: {'PASS' if checks['serial_parallel_igualdad'] else 'FAIL'}")
    print("  Serial independiente por uid: " + ("IGUAL" if fixed_order_match else "DIFERENTE"))
    print("  Nota: un orden fijo por uid puede diferir si faltan fondos; "
          "no invalida un historial linearizable.")

    print("\n[6] RENDIMIENTO")
    print(f"  Tiempo total (incluye creación de Threads): {elapsed:.6f} s")
    print(f"  Throughput (intentos/s):                 {operations / elapsed:.2f}")
    print(f"  Espera mediana por Locks:                "
          f"{statistics.median(wait_times) if wait_times else 0:.6f} ms")
    print(f"  Espera máxima por Locks:                 {max(wait_times, default=0):.6f} ms")
    if start_error:
        print(f"  error_creacion_threads: {start_error}")
    passed = all(checks.values())
    print("\n" + "=" * width)
    print(f"RESULTADO FINAL: {'PASS' if passed else 'FAIL'}")
    print("=" * width)
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
    print("\n" + "=" * 68)
    print("EVALUACIÓN EXPERIMENTAL | LOW / HIGH CONTENTION")
    print("=" * 68)
    print(f"  Balance fijo por cuenta: {args.balance}")
    print(f"  Monto máximo fijo:       {args.max_amount}")
    print(f"  Semillas:                {args.seed}..{args.seed + args.repetitions - 1}")
    print("  Serial Replay comprueba también transferencias rechazadas.")
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
        print(f"\nRESUMEN | {label} | {num_accounts} cuentas | {operations} Threads")
        print(f"  Repeticiones:          {args.repetitions}")
        print(f"  Tiempo mediano:        {statistics.median(times):.6f} s")
        print(f"  Tiempo mínimo/máximo:  {min(times):.6f} / {max(times):.6f} s")
        print(f"  Throughput mediano:    {statistics.median(throughputs):.2f} intentos/s")
    print("\nANÁLISIS: más Threads no implican mayor Throughput. Las mediciones incluyen")
    print("  Thread Creation Overhead y el registro de auditoría; GIL y Thread Scheduling")
    print("  afectan los tiempos. Lock Ordering previene Deadlock, pero no garantiza")
    print("  Fairness ni Starvation-freedom.")
    print("=" * 68)
    print(f"RESULTADO GLOBAL EXPERIMENTOS: {'PASS' if everything_ok else 'FAIL'}")
    print("=" * 68)
    return 0 if everything_ok else 1


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
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


# ============================================================================
# PREGUNTAS DE REFLEXIÓN — Challenge 14 (respuestas en comentarios)
# ============================================================================
# 1. ¿Cuáles son las condiciones necesarias para que ocurra un Deadlock?
#    Las cuatro condiciones de Coffman deben coexistir:
#    (a) Mutual Exclusion: un recurso no se comparte mientras está bloqueado;
#    (b) Hold and Wait: un Thread conserva un Lock mientras espera otro;
#    (c) No Preemption: no se retira por la fuerza un Lock ya adquirido;
#    (d) Circular Wait: existe un ciclo de Threads que esperan recursos
#        retenidos por otros Threads del mismo ciclo.
#
# 2. ¿Qué estrategia de Deadlock Prevention implementaste y por qué funciona?
#    Global Lock Ordering. Los dos Locks de una transferencia se adquieren
#    siempre por uid ascendente, independientemente de la dirección del dinero.
#    Esta disciplina impide Circular Wait, por lo que no se forma Deadlock
#    entre los Locks de cuentas usados en las transferencias.
#
# 3. ¿Por qué un único Global Lock es correcto, pero potencialmente ineficiente?
#    Garantiza Mutual Exclusion en todas las cuentas, pero serializa también
#    transferencias entre pares de cuentas independientes. Esto aumenta la
#    Contention y reduce la concurrencia disponible frente a un Lock por cuenta.
#
# 4. ¿Cómo afecta aumentar el número de Threads concurrentes?
#    Puede mejorar la concurrencia hasta cierto punto, pero también aumenta
#    Thread Creation Overhead, Context Switching y Lock Contention. No se
#    garantiza mayor Throughput; en CPython tradicional el GIL limita la
#    ejecución paralela de trabajo CPU-bound con threading.
#
# 5. ¿Qué desafíos aparecerían si una transferencia involucrara más de dos
#    cuentas?
#    Habría que adquirir TODOS los Locks involucrados por uid ascendente,
#    comprobar previamente fondos y restricciones, y efectuar el movimiento
#    completo de forma atómica antes de liberar los Locks. En sistemas con
#    persistencia serían necesarias estrategias de rollback/Atomic Transactions.
#    Este orden evita Circular Wait, pero no garantiza Fairness o ausencia de
#    Starvation; Python Lock no promete orden FIFO de adquisición.
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
