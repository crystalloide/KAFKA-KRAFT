# TP00 – Préparation du poste : WSL2, Docker, Java 21 et réglages système

> **Formation Apache Kafka 4.4 (KRaft) – projet `KAFKA-KRAFT`**
> Durée : 45 min (à réaliser avant la formation si possible)
> Poste : Windows 10/11 + **WSL2 / Ubuntu 24.04 LTS**

---

## Objectifs

- Disposer d'une distribution Ubuntu sous WSL2 correctement dimensionnée
- Installer **Docker Engine + Docker Compose**, **Java 21** et **Maven**
- Appliquer les **réglages système recommandés en production** pour Kafka
- Récupérer le projet **KAFKA-KRAFT** (fichiers de configuration, jeux de données, code Java)

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

## 1. Côté Windows : WSL2 et ressources de la VM

### 1.1 Installer / mettre à jour WSL2 (PowerShell **administrateur**)

```powershell
wsl --update
wsl --install -d Ubuntu-24.04
wsl --list --verbose
```

Affichage attendu (la colonne `VERSION` doit valoir **2**) :

```text
  NAME            STATE           VERSION
* Ubuntu-24.04    Running         2
```

> Si la distribution existait déjà en version 1 : `wsl --set-version Ubuntu-24.04 2`

### 1.2 Dimensionner la VM WSL2 : fichier `%UserProfile%\.wslconfig`

Dans PowerShell (utilisateur normal), ouvrez le fichier avec le Bloc-notes :

```powershell
notepad "$env:USERPROFILE\.wslconfig"
```

Copiez-y le contenu suivant (adaptez `memory` et `processors` à votre poste) :

```ini
# %UserProfile%\.wslconfig (côté WINDOWS) - ressources de la VM WSL2
[wsl2]
memory=12GB
processors=6
swap=4GB
localhostForwarding=true
kernelCommandLine=sysctl.vm.max_map_count=262144
```

> Le même fichier est fourni dans le dépôt : `system/wslconfig.example`.

Redémarrez WSL pour prendre en compte la configuration :

```powershell
wsl --shutdown
```

Puis relancez **Ubuntu-24.04** depuis le menu Démarrer (ou `wsl -d Ubuntu-24.04`).

---

## 2. Côté Ubuntu : systemd et outils de base

> **Toutes les commandes suivantes s'exécutent dans le terminal Ubuntu (WSL2).**

### 2.1 Vérifier que systemd est actif

```bash
cat /etc/wsl.conf
ps -p 1 -o comm=
```

Affichage attendu : `systemd`. Si ce n'est pas le cas, activez-le :

```bash
grep -q '^systemd=true' /etc/wsl.conf 2>/dev/null || printf '\n[boot]\nsystemd=true\n' | sudo tee -a /etc/wsl.conf
```

puis `wsl --shutdown` dans PowerShell et relancez Ubuntu.

### 2.2 Mettre à jour le système et installer les outils

```bash
sudo apt-get update && sudo apt-get -y upgrade
sudo apt-get install -y curl wget git jq unzip zip tree netcat-openbsd ca-certificates gnupg kcat
```

### 2.3 Installer Java 21 (LTS) et Maven

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

Définissez `JAVA_HOME` pour toutes les sessions :

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64' | sudo tee /etc/profile.d/java.sh
source /etc/profile.d/java.sh
echo $JAVA_HOME
```

> **Proxy d'entreprise :** si Maven ou Docker doivent passer par un proxy, configurez
> `~/.m2/settings.xml` (balise `<proxies>`) et `/etc/systemd/system/docker.service.d/http-proxy.conf`.

---

## 3. Installer Docker Engine et Docker Compose (dans WSL2)

> **Alternative :** si **Docker Desktop** est déjà installé sous Windows, activez
> *Settings → Resources → WSL integration → Ubuntu-24.04* et passez directement au §3.3.
> (Docker Desktop requiert une licence payante dans les grandes entreprises ; Docker Engine est libre.)

### 3.1 Ajouter le dépôt officiel Docker

```bash
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
```

### 3.2 Installer les paquets et autoriser votre utilisateur

```bash
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"
newgrp docker
```

> `newgrp docker` ouvre un shell avec le nouveau groupe. Dans les autres terminaux, fermez puis
> rouvrez la session Ubuntu (ou `wsl --shutdown`).

### 3.3 Vérifier l'installation

```bash
docker version --format 'Client {{.Client.Version}} / Serveur {{.Server.Version}}'
docker compose version
docker run --rm hello-world
```

Affichage attendu (extraits) :

```text
Client 29.x.x / Serveur 29.x.x
Docker Compose version v2.x (ou supérieure)
Hello from Docker!
```

> **Version minimale :** Docker Engine **25** (option `start_interval` des healthchecks utilisée dans le projet).

---

## 4. Récupérer le projet KAFKA-KRAFT

Choisissez **une** des options suivantes : le résultat est le même répertoire `~/KAFKA-KRAFT`.

**Option A – dépôt Git (recommandé)**

```bash
cd ~
git clone https://github.com/crystalloide/KAFKA-KRAFT.git
```

**Option B – archive fournie par le formateur**

```bash
cd ~
unzip /mnt/c/Users/<VOTRE_LOGIN_WINDOWS>/Downloads/KAFKA-KRAFT.zip
```

**Option C – génération des fichiers au fil des TP**

Chaque TP contient des blocs `cat > fichier <<'EOF_KAFKA_KRAFT' ... EOF_KAFKA_KRAFT` qui **(re)créent
à l'identique** tous les fichiers nécessaires (configurations, scripts, jeux de données, code Java).
Il suffit de créer le répertoire racine :

```bash
mkdir -p ~/KAFKA-KRAFT && cd ~/KAFKA-KRAFT
```

> Les blocs de génération peuvent être exécutés même si le dépôt a été cloné : ils réécrivent le
> même contenu. Exemple avec les deux fichiers WSL du §1 et §2 (pour référence) :

```bash
cd ~/KAFKA-KRAFT
mkdir -p system
cat > system/wsl.conf <<'EOF_KAFKA_KRAFT'
# =============================================================================
#  /etc/wsl.conf (côté Ubuntu) - active systemd (requis pour Docker Engine
#  et pour appliquer /etc/sysctl.d au démarrage)
#  Prise en compte : "wsl --shutdown" depuis PowerShell, puis relancer Ubuntu
# =============================================================================
[boot]
systemd=true
EOF_KAFKA_KRAFT
```

```bash
cd ~/KAFKA-KRAFT
mkdir -p system
cat > system/wslconfig.example <<'EOF_KAFKA_KRAFT'
# =============================================================================
#  %UserProfile%\.wslconfig (côté WINDOWS) - ressources de la VM WSL2
#  Exemple pour un poste de 16 Go de RAM (32 Go conseillés pour tout lancer)
#  Prise en compte : "wsl --shutdown" depuis PowerShell, puis relancer Ubuntu
# =============================================================================
[wsl2]
memory=12GB
processors=6
swap=4GB
# localhost de Windows redirigé vers WSL (navigateur -> Kafka UI, Grafana...)
localhostForwarding=true
# Paramètre noyau appliqué dès le démarrage de la VM
kernelCommandLine=sysctl.vm.max_map_count=262144
EOF_KAFKA_KRAFT
```

Structure du projet (option A ou B) :

```bash
cd ~/KAFKA-KRAFT && tree -L 2 -I 'target'
```

```text
KAFKA-KRAFT
├── .env                    # versions des images, CLUSTER_ID, mémoire JVM
├── docker-compose.yml      # cluster KRaft 3 contrôleurs + 3 brokers + composants optionnels
├── config/                 # JMX exporter, Prometheus, Grafana, Connect, MM2, PostgreSQL, standalone, topics
├── connect/                # connecteurs (JSON), données d'entrée/sortie, plugins téléchargés
├── data/                   # jeux de données : orders.csv, words.txt, films*.txt, schémas Avro, scripts ksqlDB
├── java/                   # kafka-java-native (API Java) et spring-kafka-demo (Spring Boot)
├── labs/                   # ces TP au format Markdown
├── scripts/                # setup, état du cluster, rolling restart, création des topics...
└── system/                 # réglages système et service systemd
```

---

## 5. Réglages système recommandés en production

Kafka repose sur le **cache de pages** du système et ouvre **un descripteur de fichier par segment et
par connexion**. Les réglages ci-dessous sont ceux recommandés pour un broker de production ; nous
les appliquons à la VM WSL2 (qui exécute les conteneurs).

### 5.1 Paramètres noyau : `/etc/sysctl.d/99-kafka.conf`

```bash
cd ~/KAFKA-KRAFT
mkdir -p system
cat > system/99-kafka.conf <<'EOF_KAFKA_KRAFT'
# =============================================================================
#  /etc/sysctl.d/99-kafka.conf - Réglages noyau recommandés pour Kafka
#  Application immédiate : sudo sysctl --system
# =============================================================================
# Swap : quasiment désactivé (la JVM ne doit jamais être "swappée")
vm.swappiness=1
# Ecriture du cache disque (page cache) : flush en tâche de fond plus tôt,
# blocage des écritures plus tard (Kafka s'appuie sur le page cache)
vm.dirty_background_ratio=5
vm.dirty_ratio=60
# Chaque segment de log ouvre 2 "memory maps" (index + timeindex) :
# indispensable avec beaucoup de partitions
vm.max_map_count=262144
# Réseau : tampons TCP plus grands (débit inter-brokers / clients)
net.core.wmem_max=2097152
net.core.rmem_max=2097152
net.ipv4.tcp_wmem=4096 65536 2048000
net.ipv4.tcp_rmem=4096 65536 2048000
net.core.somaxconn=4096
net.ipv4.tcp_max_syn_backlog=4096
# Fichiers ouverts au niveau système
fs.file-max=1000000
EOF_KAFKA_KRAFT
```

Installation et application immédiate :

```bash
sudo cp ~/KAFKA-KRAFT/system/99-kafka.conf /etc/sysctl.d/99-kafka.conf
sudo sysctl --system | grep -E 'swappiness|max_map_count|dirty_ratio'
```

Affichage attendu :

```text
vm.swappiness = 1
vm.dirty_ratio = 60
vm.max_map_count = 262144
```

| Paramètre | Valeur | Pourquoi |
|---|---|---|
| `vm.swappiness` | 1 | Ne jamais « swapper » la JVM (pauses de plusieurs secondes = broker exclu de l'ISR) |
| `vm.dirty_background_ratio` / `vm.dirty_ratio` | 5 / 60 | Ecriture du page cache vers le disque en tâche de fond, sans bloquer les producteurs |
| `vm.max_map_count` | 262144 | 2 *memory maps* par segment (index + timeindex) : nécessaire avec des milliers de partitions |
| `net.core.*mem_max`, `tcp_*mem` | 2 Mo | Tampons TCP plus grands pour la réplication et les clients distants |
| `fs.file-max` | 1 000 000 | Plafond système du nombre de fichiers ouverts |

### 5.2 Limites par utilisateur : `/etc/security/limits.d/kafka.conf`

```bash
cd ~/KAFKA-KRAFT
mkdir -p system
cat > system/kafka-limits.conf <<'EOF_KAFKA_KRAFT'
# =============================================================================
#  /etc/security/limits.d/kafka.conf - Limites par utilisateur (sessions PAM)
#  Kafka ouvre un descripteur par segment de log et par connexion : >= 100 000
# =============================================================================
*    soft    nofile    100000
*    hard    nofile    100000
*    soft    nproc     65536
*    hard    nproc     65536
EOF_KAFKA_KRAFT
```

```bash
sudo cp ~/KAFKA-KRAFT/system/kafka-limits.conf /etc/security/limits.d/kafka.conf
```

> **Sous WSL2**, ces limites PAM ne s'appliquent pas toujours au shell. Ce n'est pas bloquant : les
> conteneurs Kafka reçoivent leurs limites via `ulimits` dans `docker-compose.yml`, et le service
> systemd du TP01 via `LimitNOFILE=100000`. Vérification dans un shell : `ulimit -n`.

### 5.3 Système de fichiers (pour information)

En production : disques dédiés (SSD/NVMe), système de fichiers **XFS** ou **ext4**, option de montage
`noatime`, **pas de RAID 5/6**, un répertoire `log.dirs` par disque (JBOD) ou RAID 10.

---

## 6. IntelliJ IDEA (développeurs)

> **Version :** IntelliJ IDEA **2023.3 ou plus récente** (la prise en charge complète de Java 21 date
> de la 2023.3) ; une version récente est conseillée pour Spring Boot 4.

Les projets Java se trouvent dans `~/KAFKA-KRAFT/java/` :

```bash
ls ~/KAFKA-KRAFT/java/*/pom.xml
```

```text
/home/<user>/KAFKA-KRAFT/java/kafka-java-native/pom.xml
/home/<user>/KAFKA-KRAFT/java/spring-kafka-demo/pom.xml
```

> Si la commande répond `No such file or directory` :
> - **option A ou B** : le dépôt ou l'archive est incomplet (seuls les fichiers `.md` ont été copiés,
>   par exemple). Récupérez le projet **complet** (`KAFKA-KRAFT.zip` : répertoires `config/`, `data/`,
>   `java/`, `scripts/`… et fichiers `.env`, `docker-compose.yml`) ;
> - **option C** : c'est normal, ces projets sont créés au **TP03** (parties B.1 et C.1). Configurez
>   IntelliJ à ce moment-là.

Deux possibilités, au choix :

1. **IntelliJ sous Windows avec un projet WSL** (recommandé) :
   - *File → Open* → `\\wsl.localhost\Ubuntu-24.04\home\<user>\KAFKA-KRAFT\java\kafka-java-native\pom.xml`
     → *Open as Project* (`\\wsl$\Ubuntu-24.04\...` est un chemin équivalent) ;
   - *File → Project Structure → Project → SDK → Add SDK → JDK* :
     `\\wsl.localhost\Ubuntu-24.04\usr\lib\jvm\java-21-openjdk-amd64` ;
   - *Settings → Build, Execution, Deployment → Build Tools → Maven* : *Maven home path* =
     `\\wsl.localhost\Ubuntu-24.04\usr\share\maven`, *JDK for importer* = *Project JDK*.

   IntelliJ exécute alors Maven et les programmes **dans WSL** : `localhost:9092` désigne bien le
   cluster. Même démarche pour `java\spring-kafka-demo\pom.xml` (fenêtre séparée ou
   *File → New → Module from Existing Sources*). Si Windows Defender propose d'exclure les
   répertoires WSL de l'analyse, acceptez : l'indexation est nettement plus rapide.
2. **Terminal uniquement** : tous les programmes des TP se lancent aussi en ligne de commande
   (`./run.sh ...` ou `mvn spring-boot:run`), ce qui suffit pour réaliser les TP.

---

## 7. Vérification finale

```bash
echo "--- Java"   && java -version 2>&1 | head -1
echo "--- Maven"  && mvn -v | head -1
echo "--- Docker" && docker version --format '{{.Server.Version}}'
echo "--- Compose" && docker compose version --short
echo "--- sysctl" && sysctl vm.swappiness vm.max_map_count
echo "--- Mémoire" && free -g | head -2
echo "--- Projet" && ls ~/KAFKA-KRAFT
for f in .env docker-compose.yml scripts/setup.sh java/kafka-java-native/pom.xml java/spring-kafka-demo/pom.xml; do
  [ -e ~/KAFKA-KRAFT/$f ] && echo "OK       $f" || echo "MANQUANT $f"
done
```

> Options A et B : les 5 lignes doivent être `OK`. Une ligne `MANQUANT` signifie que le projet est
> incomplet (cf. §4). Option C : ces fichiers sont créés au fil des TP02 et TP03.

✅ Le poste est prêt. **Suite : TP01 – Introduction à Kafka et installation.**
