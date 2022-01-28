Pour l'installation de Cassandra :
1. Lancer le script ```cassandra-install.sh```
2. Configurer le fichier ```cassandra.yaml``` *
3. Lancer le script ```cassandra-config.sh```

Le fichier ```cassandra.yaml``` correspond au fichier founi par défaut lors de l'installation de Cassandra dont les lignes suivantes ont été éditées :
```
# ligne 11 : nom du cluster
cluster_name: 'Cluster Group 2'

# ligne 426 : les noeuds graines. Les 3 première machines dans l'ordre numérique ont été utilisées
```seed_provider:
    - class_name: org.apache.cassandra.locator.SimpleSeedProvider
        - seeds: tp-hadoop-1, tp-hadoop-2, tp-hadoop-5


# ligne 613 : remplacer x par le numéro de la machine correspondante
listen_address: tp-hadoop-x

# ligne 690 : remplacer x par le numéro de la machine correspondante
rpc_address: tp-hadoop-x

# ligne 962 : la politique Snitch utilisée
endpoint_snitch: GossipingPropertyFileSnitch

```