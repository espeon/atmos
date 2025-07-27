use atmos::{Bytes, CarImporter, MstStorage, Result, mst::Mst};

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

    let mst = Mst::from_car_importer(car).await?;

    // traverse the mst and print all records in order (depth-first)

    println!("root mst: {:?}", &mst.root);

    println!("MST nodes count: {}", mst.storage.len().await?);

    // DFS traversal!
    use futures::StreamExt;
    let mut stream = mst.iter().into_stream();
    let mut i = 0;
    while let Some(result) = stream.next().await {
        let _ = result.expect("valid item");
        i += 1;
    }

    println!("{i} entries in the MST in the CAR {repo_car_path}");

    Ok(())
}
