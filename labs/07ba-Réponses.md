## Questions de révision : TP07b – Kafka Streams

Réponses aux trois questions de révision du TP07b (Kafka Streams 4.4.0), illustrées avec l'application
`WordCount` du TP.

---

### 1. Pourquoi `count()` après `groupBy()` crée-t-il un topic `-repartition` ?

**Parce que l'état d'un agrégat est local à une partition.** `count()` tient un compteur par clé dans
un *state store* (RocksDB) qui appartient à **une tâche**, c'est-à-dire à **une partition**. Pour que le
compte d'un mot soit juste, **tous** les messages portant ce mot doivent arriver dans la **même**
partition, donc dans la même tâche.

**Or `groupBy()` change la clé.** Dans `WordCount`, les phrases arrivent dans `words-input` avec leur
clé d'origine (ou sans clé) : le mot `kafka` peut se trouver dans les partitions 0, 1 et 2. Après
`groupBy((key, word) -> word)`, la nouvelle clé est le mot, mais les messages sont toujours rangés selon
l'**ancienne** clé. Kafka Streams doit donc les **redistribuer** selon la nouvelle :

```text
Sous-topologie 0 (tâches 0_0, 0_1, 0_2)              Sous-topologie 1 (tâches 1_0, 1_1, 1_2)
words-input ─► flatMapValues ─► filter ─► groupBy ─► [wordcount-app-word-counts-repartition] ─► count ─► word-counts (RocksDB)
                                          (écriture, partitionnée                (lecture : chaque mot arrive
                                           par hash du nouveau mot)               toujours dans la même tâche)
```

Le topic interne `<application.id>-...-repartition` sert d'« aiguillage » : il est écrit avec la
nouvelle clé (partitionnement par hachage, comme un producer normal), puis relu par une **seconde
sous-topologie**. D'où les **6 tâches** observées en C.1 : 3 pour lire et découper, 3 pour compter.

Points à retenir :

- la redistribution n'a lieu que si une opération **avec état** suit (agrégat, jointure) : `groupBy()`
  seul ne fait que « marquer » le flux ;
- `groupByKey()` ne crée **pas** de topic de redistribution… sauf si la clé a été modifiée auparavant
  (`map`, `selectKey`, `flatMap`) : c'est pourquoi le TP conseille `mapValues` / `flatMapValues`
  quand la clé ne change pas ;
- le topic de redistribution a par défaut autant de partitions que le topic d'entrée (3 ici) ;
  `Grouped` / `Repartitioned` permettent de le nommer ou de changer ce nombre ;
- coût : une écriture et une lecture supplémentaires dans Kafka (réseau, disque, latence). Kafka
  Streams **purge** lui-même les messages déjà traités de ce topic (`repartition.purge.interval.ms`,
  30 s par défaut) : il reste petit.

---

### 2. Que contient le topic `-changelog` et pourquoi est-il compacté ?

**Contenu.** `wordcount-app-word-counts-changelog` est la **copie de sauvegarde** du *state store*
`word-counts`. À chaque mise à jour du store, Kafka Streams y écrit la nouvelle valeur :
clé = le mot, valeur = le compte courant (`Long`). La partition *i* du changelog correspond à la
tâche `1_i`.

```text
clé       valeur
kafka     1
topic     1
kafka     2      <- la valeur précédente de "kafka" devient inutile
kafka     3
```

Le cache des stores (`statestore.cache.max.bytes`, 10 Mo par défaut) regroupe les mises à jour
successives d'une même clé entre deux commits : seule la dernière est envoyée au changelog.

**À quoi il sert.** À **reconstruire** l'état local quand il n'existe pas ou plus sur la machine :

- une tâche migre vers une autre instance (arrêt d'une instance en C.1, ajout d'une instance) ;
- le répertoire d'état a été supprimé ou le disque perdu ;
- les **réplicas de secours** (`num.standby.replicas`) le relisent en continu pour être prêts à prendre
  le relais sans délai.

Au redémarrage, le fichier `.checkpoint` du répertoire de la tâche indique l'offset déjà appliqué :
seule la fin du changelog est rejouée.

**Pourquoi compacté (`cleanup.policy=compact`).** Pour reconstruire un compteur, seule la **dernière
valeur de chaque clé** compte. La compaction supprime les anciennes valeurs :

- la taille du topic dépend du **nombre de clés distinctes** (les mots), pas du nombre de mises à jour ;
- la restauration est rapide (on ne rejoue qu'une valeur par clé) ;
- la conservation est illimitée sans croissance sans fin, ce qu'une rétention par durée
  (`cleanup.policy=delete`) ne permettrait pas : elle finirait par effacer des compteurs toujours
  valides ;
- une valeur `null` (tombstone) supprime la clé du store et, après compaction, du changelog.

Cas particulier : pour un store **fenêtré** (agrégat par fenêtre de temps), le changelog est en
`compact,delete` : les anciennes fenêtres expirent (durée de la fenêtre + délai de grâce +
`windowstore.changelog.additional.retention.ms`, 1 jour par défaut).

---

### 3. Combien d'instances actives au maximum pour un topic d'entrée à 3 partitions ?

**Trois** pour une topologie simple : l'unité de parallélisme est la **tâche**, et une sous-topologie
compte **autant de tâches que de partitions** de son topic d'entrée. Chaque tâche est exécutée par
**un seul** thread à la fois. Avec 3 partitions, au plus 3 threads (par exemple 3 instances avec
`num.stream.threads=1`) ont du travail. Une 4e instance rejoint le groupe mais ne reçoit **aucune
tâche active** : elle reste inactive, ou n'héberge que des réplicas de secours si
`num.standby.replicas` ≥ 1.

**Nuance pour `WordCount`** : la topologie a **2 sous-topologies** de 3 tâches chacune (lecture et
découpage, puis comptage après redistribution), soit **6 tâches**. Jusqu'à 6 threads peuvent donc
recevoir une tâche. Mais chaque étape reste limitée à **3** traitements en parallèle : au-delà de
6 threads, les instances supplémentaires sont inactives.

Le décompte porte sur les **threads**, pas sur les instances : 1 instance avec 3 threads ou 3 instances
avec 1 thread offrent le même parallélisme. Pour aller plus loin, il faut **plus de partitions** dans le
topic d'entrée. Sur une application avec état, ce changement modifie la répartition des clés : il
impose de **réinitialiser** l'application (`kafka-streams-application-reset.sh`, C.2). D'où l'intérêt de
prévoir assez de partitions dès le départ.
