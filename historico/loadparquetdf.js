const startTime = Date.now();
const fedDB = db.getSiblingDB("dbimport");

print("Iniciando migração de 35M de linhas em background...");

try {
    fedDB.s3import.aggregate([
        {
            $out: {
                atlas: {
                    clusterName: "ClusterUS",
                    db: "production_db",
                    coll: "simulated_35M"
                }
            }
        }
    ], { allowDiskUse: true });

    const duration = ((Date.now() - startTime) / 60000).toFixed(2);
    print("------------------------------------------");
    print("✅ Sucesso! Tempo total: " + duration + " minutos");
    print("------------------------------------------");
} catch (err) {
    print("❌ Erro na migração: " + err);
}
