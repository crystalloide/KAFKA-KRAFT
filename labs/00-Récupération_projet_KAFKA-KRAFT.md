## Vue d'ensemble de l'environnement de formation

```text
 Windows (navigateur, IntelliJ)                      WSL2 - Ubuntu 24.04
 ─────────────────────────────                      ──────────────────────────────────────────────
  http://localhost:8080  Kafka UI   ──────────►      Docker Engine  (réseau "kafka-net")
  http://localhost:3000  Grafana                     ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
  http://localhost:9090  Prometheus                  │ controller-1 │ │ controller-2 │ │ controller-3 │  quorum KRaft
                                                     └──────────────┘ └──────────────┘ └──────────────┘
  IntelliJ / Maven  ──── localhost:9092 ────►        ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
                         localhost:9094              │   broker-1   │ │   broker-2   │ │   broker-3   │  données
                         localhost:9096              └──────────────┘ └──────────────┘ └──────────────┘
                                                     + Kafka UI, Prometheus, Grafana, Connect, Schema Registry,
                                                       ksqlDB, MirrorMaker 2... (selon les TP)
                                                     /opt/kafka : binaires Kafka 4.4.0 (outils CLI + noeud TP01)
```

| Service | Port(s) hôte | TP |
|---|---|---|
| Noeud Kafka "standalone" (binaire) | 9192 (clients), 9193 (contrôleur) | TP01 |
| Brokers du cluster Docker | 9092, 9094, 9096 (+ 9098 pour broker-4) | TP02 → TP08 |
| Kafka UI | 8080 | TP02 → TP08 |
| Kafka Connect (API REST) | 8083 | TP07a |
| Schema Registry | 8081 | TP07c |
| ksqlDB | 8088 | TP07d |
| Cluster de secours (MirrorMaker 2) | 9292 | TP07e |
| Prometheus / Grafana / kafka-exporter | 9090 / 3000 / 9308 | TP05 |
| Application Spring Boot | 8085 | TP03, TP05 |
| AKHQ (console alternative) | 8090 | TP04 |

> **Mémoire conseillée :** 16 Go de RAM minimum sur le poste (12 Go alloués à WSL2), 32 Go pour être à l'aise.
> Les composants optionnels sont démarrés par **profils** Docker Compose et arrêtés quand on n'en a plus besoin.

---

### 0. Vérifier les pré-requis d'installation

```bash
sudo apt-get install -y openjdk-21-jdk maven
java -version
mvn -v
```

Affichage attendu (extraits) :

```text
openjdk version "21.0.x" ...
Apache Maven 3.8.x ...
Java version: 21.0.x, vendor: Ubuntu, runtime: /usr/lib/jvm/java-21-openjdk-amd64
```

```bash
echo $JAVA_HOME
```

```bash
docker version --format 'Client {{.Client.Version}} / Serveur {{.Server.Version}}'
docker compose version
docker run --rm hello-world
```

---

## 1. Récupérer le projet KAFKA-KRAFT

Choisissez **une** des options suivantes : le résultat est le même répertoire `~/KAFKA-KRAFT`.

**Option A – dépôt Git (recommandé)**

```bash
cd ~
rm -Rf ~/KAFKA-KRAFT
git clone https://github.com/crystalloide/KAFKA-KRAFT.git
cd ~/KAFKA-KRAFT
ls
```
