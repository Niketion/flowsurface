# Adaptive Volume Bubbles

Volume Bubbles visualizza burst di esecuzioni aggressive, non semplici price bin. Un trade è una
singola esecuzione; un price bin somma tutte le esecuzioni allo stesso prezzo; uno smart cluster
unisce invece soltanto trade della stessa candela vicini nel tempo e nel prezzo. Il cluster conserva
VWAP, tempo volume-weighted, buy/sell, delta, numero di trade e trade massimo.

La pipeline pura `cluster_volume_bubble_trades` è condivisa da live, storico e replay. Quando i raw
trade completi sono disponibili hanno precedenza; in alternativa viene usata esclusivamente la
summary v2 già clusterizzata. Raw e summary non vengono mai sommati. Le summary v1, basate su
candela/prezzo, vivono in una tabella differente e non sono interpretate come cluster v2.

## Soglia e stabilità

- **Fixed** usa `min_qty`.
- **AdaptivePercentile** usa il percentile rolling dei cluster nella finestra configurata.
- **Hybrid** usa il massimo tra percentile e `min_qty`.

Durante il warm-up il floor assoluto evita NaN e soglie instabili. La soglia live viene aggiornata al
massimo una volta al secondo e soltanto per variazioni almeno del 10%. L'ID del cluster deriva dagli
anchor stabili del burst, quindi non cambia mentre il cluster corrente cresce. Una baseline per lato
può essere usata quando ha campioni sufficienti; altrimenti si usa la distribuzione combinata.

## Gerarchia visuale

Il volume e il percentile dominano l'importance score; dominance e trade count aggiungono soltanto
piccoli bonus spiegabili. Dopo la soglia si applicano il budget per candela, il budget globale del
viewport, collisioni orizzontali deterministiche e il budget label. `ExtremeOnly` etichetta soltanto
gli eventi oltre il percentile label; le altre bubble restano prive di testo.

Il raggio usa compressione logaritmica rispetto alla soglia e interpolazione sull'area del cerchio.
Resta nei limiti configurati ed è resistente agli outlier. Fill trasparente, bordo più leggibile e
colori derivati dal tema preservano candele, wick e overlay. I cluster con dominance debole sono
neutri. L'age fading scende gradualmente fino a circa il 58%, senza nascondere lo storico.

## Price response

L'analisi opzionale classifica la risposta dopo un orizzonte realmente trascorso come
`FollowThrough`, `Stalled`, `Reversed`, `Pending` o `Neutral`. Non usa dati futuri in live e resta
secondaria rispetto al volume. `Stalled` può essere compatibile con assorbimento passivo, ma non lo
dimostra e non identifica automaticamente un iceberg.

## Limiti interpretativi

Una bubble indica attività aggressiva aggregata. Non identifica automaticamente apertura o chiusura
di una posizione. Il risultato dipende dalla qualità, dall'ordine e dalla completezza dei trade
forniti dall'exchange; dati intrabar incompleti riducono la precisione temporale del clustering.
