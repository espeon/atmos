use atmos::{Bytes, CarImporter, Result, mst::Mst};

#[tokio::main]
async fn main() -> Result<()> {
    let repo_car_path = "repo.car";
    let repo_car_bytes = std::fs::read(repo_car_path)?;

    let repo_car_bytes: Bytes = Bytes::from(repo_car_bytes);

    let mut car = CarImporter::new();
    car.import_from_bytes(repo_car_bytes).await?;

    println!(
        "Root CIDs: {:?}, {} items long",
        car.roots(),
        car.roots().len()
    );
    println!("Total blocks: {}", car.len());
    println!();

    let mst: Mst = car.try_into()?;

    // traverse the mst and print all records in order (depth-first)

    println!("root mst: {:?}", &mst.root);

    println!("MST nodes count: {}", mst.nodes.len());

    // DFS traversal!
    for (i, (cid, node)) in mst.iter().enumerate() {
        println!("{}. CID: {}, Node: {:?}", i, cid, node);
    }

    Ok(())
}
