# Mode Opératoire : Configurer Java 21 et Maven dans WSL2 ou une VM Ubuntu :

Ce guide résume les étapes nécessaires pour installer Java 21, configurer le système d'alternatives et faire fonctionner Maven correctement sous WSL2 (Ubuntu).

---


### 1.1 Mettre à jour le système et installer les outils

```bash
sudo apt-get update && sudo apt-get -y upgrade
sudo apt-get install -y curl wget git jq unzip zip tree netcat-openbsd ca-certificates gnupg kcat
```

### 1.2 Installer Java 21 (LTS) et Maven

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


### 1.3 : Configurer les alternatives JAVA et JAVAC : 

Exécutez les commandes suivantes pour enregistrer JAVA 21 auprès du système :

```bash
sudo apt update && sudo apt install -y openjdk-21-jdk
sudo update-alternatives --install /usr/bin/java java /usr/lib/jvm/java-21-openjdk-amd64/bin/java 21
sudo update-alternatives --config java
```
*Note : Dans le menu interactif qui s'affiche, tapez le numéro correspondant à Java 21 et validez avec **Entrée**.*

```bash
sudo update-alternatives --config javac
```
*Note : Dans le menu interactif qui s'affiche, tapez le numéro correspondant à Java 21 et validez avec **Entrée**.*

---

### 1.4 : Configurer `JAVA_HOME` de manière permanente

1. Ouvrez votre fichier de configuration Bash :
   ```bash
   vi ~/.bashrc
   ```

2. Ajoutez ou mettez à jour les lignes suivantes tout en bas du fichier :
   ```bash
   export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
   export PATH=$JAVA_HOME/bin:$PATH
   ```

Rappel sur l'éditeur :

- Avec **nano** : Enregistrez et quittez (`Ctrl + O`, puis `Entrée`, puis `Ctrl + X`).
- Avec **vi** : Enregistrez et quittez (quitter le mode insertion `I`, puis `:wq`, puis `Entrée`).
   
3. Appliquez les modifications à votre session en cours :
   ```bash
   source ~/.bashrc
   ```

---

### 2.1 : Valider l'installation de JAVA et MAVEN : 

Vérifiez que Java 21 est bien actif sur votre système :
```bash
java -version
```
*(Le retour doit indiquer une version `openjdk version "21..."`).*

Vérifiez que le Home Java est bien renseigné :
```bash
echo $JAVA_HOME
```

Vérifiez ensuite que Maven fonctionne et utilise la bonne version de Java :
```bash
mvn -v
```
*(Maven doit démarrer sans erreur de variable d'environnement et afficher le runtime Java 21).*

---

### 3.1 : (optionnel) : désactivation UFW et Apparmor (en Dévt uniquement) : 

```bash
sudo systemctl disable apparmor
sudo systemctl disable ufw
sudo systemctl stop apparmor
sudo systemctl stop ufw
sudo systemctl status apparmor
sudo systemctl status ufw
```
