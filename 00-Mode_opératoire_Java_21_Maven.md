# Mode Opératoire : Configurer Java 21 et Maven dans WSL2

Ce guide résume les étapes nécessaires pour installer Java 21, configurer le système d'alternatives et faire fonctionner Maven correctement sous WSL2 (Ubuntu).

---

### Étape 1 : Installer Java 21 et configurer les alternatives

Exécutez les commandes suivantes dans votre terminal WSL2 pour installer le JDK 21 et l'enregistrer auprès du système :

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

### Étape 2 : Configurer `JAVA_HOME` de manière permanente

1. Ouvrez votre fichier de configuration Bash :
   ```bash
   nano ~/.bashrc
   ```

2. Ajoutez ou mettez à jour les lignes suivantes tout en bas du fichier :
   ```bash
   export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
   export PATH=$JAVA_HOME/bin:$PATH
   ```

3. Enregistrez et quittez (`Ctrl + O`, puis `Entrée`, puis `Ctrl + X`).

4. Appliquez les modifications à votre session en cours :
   ```bash
   source ~/.bashrc
   ```

---

### Étape 3 : Valider l'installation

Vérifiez que Java 21 est bien actif sur votre système :
```bash
java -version
```
*(Le retour doit indiquer une version `openjdk version "21..."`).*

Vérifiez ensuite que Maven fonctionne et utilise la bonne version de Java :
```bash
mvn -v
```
*(Maven doit démarrer sans erreur de variable d'environnement et afficher le runtime Java 21).*
