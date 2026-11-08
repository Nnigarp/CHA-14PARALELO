# Challenge 14 — Concurrent Bank Transfers with Deadlock Prevention

**Curso:** Computación en Paralelo — UTB  
**Estudiante:** Nilver García Pimentel

## Entregable para SAVIO
El archivo **`Challenge14_Nilver_Garcia_Pimentel.txt`** contiene **código Python completo y ejecutable**, idéntico al archivo `.py`. No depende de módulos locales ni de evidencias externas. Para ejecutarlo directamente como script de Python: `python Challenge14_Nilver_Garcia_Pimentel.txt --help`. Para SAVIO se entrega **solamente el TXT**, salvo indicación distinta del docente.

## Requisitos
Python 3.9+; solo standard library (`argparse`, `random`, `statistics`, `threading`, `time`, `dataclasses`).

## Comandos (PowerShell)
```powershell
python .\Challenge14_Nilver_Garcia_Pimentel.py --help
python .\Challenge14_Nilver_Garcia_Pimentel.py --accounts 2 --balance 1000 --transfers 4 --max-amount 100 --logs
python .\Challenge14_Nilver_Garcia_Pimentel.py --accounts 10 --balance 1000 --transfers 100 --max-amount 100
python .\Challenge14_Nilver_Garcia_Pimentel.py --accounts 2 --balance 0 --transfers 12 --max-amount 100 --logs
python .\Challenge14_Nilver_Garcia_Pimentel.py --experiments --repetitions 5
python .\Challenge14_Nilver_Garcia_Pimentel.txt --accounts 2 --balance 1000 --transfers 4 --max-amount 100
```

## Diseño
- Cada `Account` mantiene saldo y un `Lock` independiente.
- `Global Lock Ordering`: adquirir ambos `Lock` en orden ascendente por `uid`; elimina `Circular Wait`, condición necesaria de `Deadlock`.
- La comprobación de fondos y el débito/crédito ocurren dentro de la misma `Critical Section`.
- Todos los `Thread` arrancados reciben `join()`; no se introducen `sleep()` artificiales.
- La auditoría reconstruye el saldo final de cada cuenta únicamente a partir de transferencias confirmadas. También verifica conservación del dinero, saldos no negativos, finalización, contabilización y ausencia de excepciones.
- Las cinco `Reflection Questions` se responden **dentro del código como comentarios**, tal como requiere el reto.

## Experimental Evaluation
`--experiments` compara **100 y 2 cuentas** con **40 y 120 threads**, y ejecuta cinco repeticiones por defecto. Cada repetición informa su resultado; el resumen muestra mediana, mínimo y máximo del tiempo, y mediana del `Throughput`.

**Límites de interpretación:** `Low/High Contention` describe la configuración, no una prueba de tiempo de espera elevado. Los tiempos incluyen `Thread Creation Overhead` y pueden depender del `Thread Scheduling`, GIL y del equipo. `Global Lock Ordering` prueba prevención de `Deadlock` por diseño, pero no garantiza `Fairness` o ausencia de `Starvation`.

## Evidencia
Los logs de ejecuciones en el computador del estudiante deben generarse con estos comandos y conservarse sin modificación. Los resultados obtenidos en otro entorno no deben atribuirse al Lenovo.

## Referencias
- Raynal, M. (2013), *Concurrent Programming: Algorithms, Principles, and Foundations*.
- Python, documentación de `threading`: https://docs.python.org/3/library/threading.html
