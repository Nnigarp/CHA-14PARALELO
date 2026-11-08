#!/usr/bin/env python3
"""Challenge 14: Concurrent Bank Transfers with Deadlock Prevention.

Uso: python Challenge14_Nilver_Garcia_Pimentel.py --help
Python 3.9+; únicamente standard library. Mensajes en español, términos técnicos en inglés.
"""
import argparse
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
class Transaction:
    uid: int
    source: int
    target: int
    amount: int


@dataclass
class Outcome:
    status: str = 'NO_INICIADA'
    wait_ns: int = 0
    error: str = ''


def transfer(accounts, tx):
    """Global Lock Ordering; la Critical Section protege saldo y transferencia.

    Se mide el Lock Wait Time, sin usar sleep artificiales ni Global Lock.
    """
    source, target = accounts[tx.source], accounts[tx.target]
    first, second = sorted((source, target), key=lambda a: a.uid)
    start = time.perf_counter_ns()
    with first.lock:
        with second.lock:
            wait_ns = time.perf_counter_ns() - start
            if source.balance < tx.amount:
                return 'FONDOS_INSUFICIENTES', wait_ns
            source.balance -= tx.amount
            target.balance += tx.amount
            return 'OK', wait_ns


def make_workload(num_accounts, transfers, max_amount, seed):
    rng = random.Random(seed)
    tasks = []
    for uid in range(transfers):
        source = rng.randrange(num_accounts)
        target = (source + rng.randrange(1, num_accounts)) % num_accounts
        tasks.append(Transaction(uid, source, target, rng.randint(1, max_amount)))
    return tasks


def run_case(num_accounts, balance, transfers, max_amount, seed, logs=False, title='Prueba'):
    accounts = [Account(i, balance) for i in range(num_accounts)]
    workload = make_workload(num_accounts, transfers, max_amount, seed)
    outcomes = [Outcome() for _ in workload]
    ready = threading.Event()
    threads = []
    startup_error = None

    def worker(tx):
        try:
            ready.wait()
            status, waited = transfer(accounts, tx)
            outcomes[tx.uid] = Outcome(status, waited)
        except Exception as exc:
            outcomes[tx.uid] = Outcome('ERROR', error=repr(exc))

    start = time.perf_counter()
    try:
        for tx in workload:
            t = threading.Thread(target=worker, args=(tx,), name=f'T{tx.uid:05d}')
            t.start()
            threads.append(t)
    except RuntimeError as exc:
        startup_error = repr(exc)
    finally:
        ready.set()
        for t in threads:
            t.join()
    elapsed = time.perf_counter() - start

    real = [a.balance for a in accounts]
    audited = [balance] * num_accounts
    counts = {'OK': 0, 'FONDOS_INSUFICIENTES': 0, 'ERROR': 0, 'NO_INICIADA': 0}
    for tx, result in zip(workload, outcomes):
        counts[result.status] = counts.get(result.status, 0) + 1
        if result.status == 'OK':
            audited[tx.source] -= tx.amount
            audited[tx.target] += tx.amount

    # Independiente del orden de ejecución: se reconstruyen saldos usando
    # exclusivamente las transferencias confirmadas, no los balances del banco.
    tests = {
        'conservacion_dinero': sum(real) == num_accounts * balance,
        'saldos_no_negativos': all(x >= 0 for x in real),
        'auditoria_por_cuenta': real == audited,
        'todos_los_threads_finalizaron': len(threads) == transfers and all(not t.is_alive() for t in threads),
        'operaciones_contabilizadas': counts['OK'] + counts['FONDOS_INSUFICIENTES'] == transfers,
        'sin_excepciones': startup_error is None and counts['ERROR'] == 0,
    }
    waits = [o.wait_ns / 1_000_000 for o in outcomes if o.status in ('OK', 'FONDOS_INSUFICIENTES')]
    print(f'\n=== {title} ===')
    print(f'cuentas={num_accounts} saldo_inicial={balance} threads={transfers} max_amount={max_amount} seed={seed}')
    if logs:
        for tx, result in zip(workload, outcomes):
            print(f'T{tx.uid:05d} origen=A{tx.source} destino=A{tx.target} monto={tx.amount} '
                  f'estado={result.status} lock_wait_ms={result.wait_ns/1e6:.6f}'
                  + (f' error={result.error}' if result.error else ''))
    print(f'operaciones: intentadas={transfers} realizadas={counts["OK"]} '
          f'rechazadas={counts["FONDOS_INSUFICIENTES"]} errores={counts["ERROR"]}')
    print(f'total_inicial={num_accounts * balance} total_final={sum(real)}')
    print(f'tiempo_seg={elapsed:.6f} throughput_ops_seg={transfers / elapsed:.2f}')
    print(f'lock_wait_mediana_ms={statistics.median(waits) if waits else 0:.6f} '
          f'lock_wait_max_ms={max(waits, default=0):.6f}')
    for name, result in tests.items():
        print(f'{name}: {"PASS" if result else "FAIL"}')
    if startup_error:
        print(f'error_creacion_threads={startup_error}')
    print('deadlock_prevention: Global Lock Ordering (uid ascendente): rompe Circular Wait')
    passed = all(tests.values())
    print(f'RESULTADO: {"PASS" if passed else "FAIL"}')
    return passed, elapsed, transfers / elapsed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--accounts', type=int, default=10, help='número de cuentas (mínimo 2)')
    parser.add_argument('--balance', type=int, default=1000, help='saldo inicial entero no negativo')
    parser.add_argument('--transfers', type=int, default=100, help='número de operaciones y threads')
    parser.add_argument('--max-amount', type=int, default=100, help='importe máximo entero positivo')
    parser.add_argument('--seed', type=int, default=2026, help='semilla reproducible')
    parser.add_argument('--logs', action='store_true', help='mostrar cada transferencia')
    parser.add_argument('--experiments', action='store_true', help='comparar cuatro escenarios')
    parser.add_argument('--repetitions', type=int, default=5, help='repeticiones por escenario experimental')
    args = parser.parse_args()
    if args.accounts < 2 or args.balance < 0 or args.transfers < 1 or args.max_amount < 1 or args.repetitions < 1:
        parser.error('Se requiere accounts>=2, balance>=0, transfers>=1, max-amount>=1 y repetitions>=1')
    if not args.experiments:
        ok, _, _ = run_case(args.accounts, args.balance, args.transfers, args.max_amount,
                            args.seed, args.logs, 'Escenario personalizado')
        return 0 if ok else 1

    # Se mantiene constante el número de threads en cada par comparado.
    scenarios = [('Low Contention', 100, 40), ('High Contention', 2, 40),
                 ('Low Contention', 100, 120), ('High Contention', 2, 120)]
    all_ok = True
    for label, n_accounts, n_threads in scenarios:
        times, rates = [], []
        for repetition in range(args.repetitions):
            ok, elapsed, rate = run_case(n_accounts, args.balance, n_threads,
                                         args.max_amount, args.seed + repetition,
                                         args.logs, f'{label}: {n_accounts} cuentas / {n_threads} threads | corrida {repetition + 1}')
            all_ok = all_ok and ok
            times.append(elapsed)
            rates.append(rate)
        print(f'RESUMEN {label} ({n_accounts} cuentas, {n_threads} threads): '
              f'corridas={args.repetitions} mediana_tiempo_s={statistics.median(times):.6f} '
              f'min_s={min(times):.6f} max_s={max(times):.6f} '
              f'mediana_throughput_ops_s={statistics.median(rates):.2f}')
    print(f'\nRESULTADO GLOBAL EXPERIMENTOS: {"PASS" if all_ok else "FAIL"}')
    print('INTERPRETACION: los tiempos incluyen Thread Creation Overhead; '
          'CPython GIL y Thread Scheduling pueden afectar los resultados. '
          'Global Lock Ordering evita Deadlock, pero no garantiza Fairness ni Starvation-freedom.')
    return 0 if all_ok else 1


# REFLECTION QUESTIONS (comentarios exigidos por el docente):
# 1. Condiciones de Coffman: Mutual Exclusion, Hold and Wait, No Preemption
#    y Circular Wait. El Deadlock requiere que concurran las cuatro.
# 2. Global Lock Ordering: todos los threads adquieren los Lock por uid
#    ascendente, incluso si el dinero se transfiere en sentido inverso.
#    Por tanto no se puede formar un ciclo de esperas (Circular Wait).
# 3. Un único Global Lock mantiene Correctness, pero serializa incluso
#    transferencias entre cuentas independientes; limita la concurrencia.
# 4. Más threads pueden aumentar Contention, Thread Scheduling Overhead y
#    Context Switching; el Throughput no necesariamente mejora. El GIL de
#    CPython tradicional limita el paralelismo CPU-bound.
# 5. Con más de dos cuentas, adquirir todos los Lock por uid ascendente,
#    validar las precondiciones y aplicar la actualización atómicamente.
#    Fallas de persistencia requieren una estrategia transaccional adicional.
#
# REFERENCIAS: Raynal (2013), Concurrent Programming: Algorithms, Principles,
# and Foundations; Python threading: https://docs.python.org/3/library/threading.html


if __name__ == '__main__':
    raise SystemExit(main())
