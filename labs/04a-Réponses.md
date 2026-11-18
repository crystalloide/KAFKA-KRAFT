# Questions de révision : TP04 – Administrer un cluster Kafka

Réponses aux quatre questions de révision du TP04 (Apache Kafka 4.4.0, mode KRaft).

---

### 1. Quelle est la différence entre `message.max.bytes` et `max.message.bytes` ?

Les deux paramètres fixent **la même limite** : la taille maximale d'un **lot de messages**
(*record batch*) accepté par le broker, mesurée **après compression**. Ils ne diffèrent que par leur
**portée** :

| | `message.max.bytes` | `max.message.bytes` |
|---|---|---|
| Niveau | **broker** (valeur par défaut pour tous les topics) | **topic** (surcharge pour un topic donné) |
| Modification | `kafka-configs.sh --entity-type brokers --entity-default` (dynamique, tout le cluster) | `kafka-configs.sh --entity-type topics --entity-name <topic>` |
| Défaut | 1 048 588 octets (1 Mio + 12 octets d'en-tête de journal) | hérite de `message.max.bytes` |

Le nom différent est un héritage historique : la plupart des paramètres de topic reprennent le nom du
paramètre broker sans le préfixe `log.` (`log.retention.ms` → `retention.ms`), mais celui-ci fait
exception. La règle est la même : **le paramètre de topic l'emporte** sur celui du broker.

Pour faire passer des messages plus gros (cf. B.4), trois réglages doivent être cohérents :

1. producer : `max.request.size` (sinon refus **côté client**) ;
2. topic : `max.message.bytes` (sinon refus **côté broker** : `RecordTooLargeException`) ;
3. consumer : `max.partition.fetch.bytes` / `fetch.max.bytes`. Ils ne sont plus bloquants depuis
   KIP-74 (un lot plus gros est quand même renvoyé), de même que `replica.fetch.max.bytes` entre brokers.

Bonne pratique : augmenter la limite **par topic**, pour les seuls topics qui en ont besoin, plutôt que
pour tout le cluster ; au-delà de quelques Mo, publier une **référence** vers un stockage externe
(pattern *claim check*).

---

### 2. Pourquoi la compaction ne s'applique-t-elle jamais au segment actif ?

**Raison technique.** Le *log cleaner* ne modifie pas un segment en place : il **recopie** les
segments à nettoyer dans de nouveaux fichiers (`.cleaned`) en ne gardant que la dernière valeur de
chaque clé, puis les **substitue** aux anciens (`.swap`). Cela suppose des segments **immuables**. Or
le segment actif reçoit des écritures en permanence : on ne peut pas le réécrire pendant que les
producteurs y ajoutent des messages.

**Raison fonctionnelle.** Kafka garantit qu'un consommateur qui suit la fin du journal (la « tête »)
voit **toutes** les mises à jour, dans l'ordre. Seule la partie ancienne (la « queue », déjà nettoyée)
est réduite à la dernière valeur par clé.

Le *cleaner* s'arrête donc au premier offset « non nettoyable », c'est-à-dire le plus petit de :

- la base du **segment actif** ;
- le **dernier offset stable** (*LSO* : pas de compaction au-delà d'une transaction encore ouverte) ;
- le premier segment plus récent que `min.compaction.lag.ms`.

**Conséquence pratique.** Sur un topic compacté peu alimenté, le segment actif peut rester ouvert
très longtemps (`segment.ms` = 7 jours, `segment.bytes` = 1 Gio par défaut) : aucune compaction
n'a lieu. D'où les valeurs agressives du TP (`segment.ms=10000`). En production, on règle plutôt
`max.compaction.lag.ms` : pour un topic compacté, Kafka ferme le segment actif au plus tard après
min(`segment.ms`, `max.compaction.lag.ms`) (fermeture déclenchée par l'écriture suivante).

---

### 3. Dans quel ordre mettre à jour contrôleurs et brokers ? Quand lancer `kafka-features.sh upgrade` ?

**Ordre recommandé :**

1. lire les notes de mise à jour (*Upgrade notes*) de la version cible ;
2. **contrôleurs** d'abord, **un par un**, en vérifiant après chacun que le quorum est sain
   (`kafka-metadata-quorum.sh describe --status` : leader présent, `MaxFollowerLag` revenu à 0).
   Avec 3 contrôleurs, il n'en manque jamais plus d'un : la majorité (2 sur 3) est conservée ;
3. **brokers** ensuite, **un par un**, en attendant **0 partition sous-répliquée** avant de passer au
   suivant (c'est ce que fait `./scripts/rolling-restart.sh recreate all`) ;
4. les **clients** à leur rythme : la compatibilité client / broker est bidirectionnelle (Kafka 4.x
   exige toutefois des clients en version 2.1 ou plus).

Pendant toute cette phase, les nœuds en nouvelle version continuent de parler le « langage » de
l'ancienne : la version des métadonnées (`metadata.version`) reste à l'ancien niveau, ce qui bloque
l'usage des nouveaux formats d'enregistrement et des nouvelles RPC. La documentation Apache décrit
simplement un redémarrage « un nœud à la fois » ; mettre à jour le quorum de contrôleurs **avant**
les brokers est l'ordre recommandé par les distributions (Confluent notamment) : les contrôleurs, qui
pilotent les métadonnées, connaissent ainsi la nouvelle version avant les brokers qui les consomment.

**Quand lancer `kafka-features.sh upgrade` ?** Seulement **à la fin** :

- quand **tous** les contrôleurs et brokers tournent en nouvelle version ;
- et après une période d'observation (performances, erreurs, clients), car c'est l'étape qui
  **active** les nouveautés de la version… et qui est **irréversible** dès que la nouvelle
  `metadata.version` contient des changements de format de métadonnées.

```bash
kafka-features.sh --bootstrap-server $BOOTSTRAP describe
kafka-features.sh --bootstrap-server $BOOTSTRAP upgrade --release-version 4.4 --dry-run
kafka-features.sh --bootstrap-server $BOOTSTRAP upgrade --release-version 4.4
```

> `--release-version` relève **toutes** les *features* à leur niveau par défaut pour cette version
> (`metadata.version`, mais aussi `kraft.version`, `group.version`, `transaction.version`…) : le
> `--dry-run` montre exactement ce qui changera. Pour ne relever qu'une seule feature :
> `--feature metadata.version=<niveau>`, où le niveau numérique est donné par
> `kafka-features.sh --bootstrap-server $BOOTSTRAP version-mapping --release-version 4.4`.

Tant que cette commande n'a pas été lancée, un retour arrière vers l'ancienne version du logiciel
reste possible (en redéployant l'ancienne image, nœud par nœud).

---

### 4. Un client Java sous Windows (hors WSL) peut-il utiliser `localhost:9092` ? Pourquoi ?

**Oui, sur le poste de formation**, pour deux raisons qui doivent être réunies :

1. **Le premier contact (*bootstrap*) aboutit.** Docker Engine tourne dans la VM WSL2 et y publie les
   ports 9092, 9094 et 9096. WSL2 redirige automatiquement le `localhost` de Windows vers ces ports
   (`localhostForwarding=true` dans `.wslconfig`, comportement par défaut ; c'est le même mécanisme qui
   permet d'ouvrir Kafka UI sur `http://localhost:8080` dans le navigateur Windows).
2. **Les adresses annoncées sont joignables depuis Windows.** Le *bootstrap* ne sert qu'à obtenir les
   métadonnées : le client se connecte ensuite **directement** au leader de chaque partition, à
   l'adresse **annoncée** par le listener utilisé. Le listener EXTERNAL annonce `localhost:9092`,
   `localhost:9094` et `localhost:9096`, que Windows redirige également vers WSL.

Si le listener EXTERNAL annonçait `broker-1:9092`, le *bootstrap* réussirait (port publié) mais les
connexions suivantes échoueraient : le nom `broker-1` n'existe que dans le réseau Docker (cf. partie F,
« erreur classique »).

**Limites :**

- la redirection n'existe que lorsque la distribution WSL est **démarrée** ;
- un autre programme Windows qui occuperait déjà le port 9092 prendrait la priorité ;
- depuis une **autre machine** du réseau, `localhost` désigne cette machine elle-même : il faudrait un
  listener supplémentaire annonçant le nom DNS ou l'adresse IP du poste Windows, et ouvrir le pare-feu
  (ou utiliser le mode réseau `networkingMode=mirrored` de WSL2).

Règle générale : **un listener par réseau d'accès**, chacun annonçant une adresse **résolvable et
joignable depuis les clients de ce réseau**.
