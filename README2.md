```text
00-Preparation-environnement-WSL2.md
01-Introduction-et-installation-Kafka.md
02-Cluster-KRaft.md
03-API-Producer-Consumer-Java-Spring.md
04-Administration-cluster.md
05-Supervision-et-optimisation.md
06-Securite-synthese.md
07a-Kafka-Connect.md
07b-Kafka-Streams.md
07c-Schema-Registry.md
07d-ksqlDB.md
07e-MirrorMaker2.md
08-Conclusion.md
```

_____

#####  Installation

1°) Lancer Ubuntu sur la machine windows (Ubuntu 24.4.1 dans cet exemple) 
```bash
ubuntu
```

2°) Prendre le user souhaité :

```bash
su - user
```

3°) Regarder où nous sommes :

```bash
pwd
```

```bash
cd ~
sudo rm -Rf ~/KAFKA-KRAFT
git clone https://github.com/crystalloide/KAFKA-KRAFT
```

```bash
cd ~/KAFKA-KRAFT
pwd
```
