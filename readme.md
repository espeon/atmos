# Atmos
Light Atproto-style merkle search tree implementation in Rust.

# Example Usage

```rs
use atmos::{Bytes, CarImporter, mst::Mst};

#[tokio::main]
async fn main() -> Result<(), String> {
    let repo_car_path = "repo.car";
    let repo_car_bytes =
        std::fs::read(repo_car_path).map_err(|e| format!("Failed to read CAR file: {}", e))?;

    let repo_car_bytes: Bytes = Bytes::from(repo_car_bytes);

    let mut car = CarImporter::new();
    car.import_from_bytes(repo_car_bytes)
        .await
        .map_err(|e| format!("Failed to import CAR file: {}", e))?;

    println!(
        "Root CIDs: {:?}, {} items long",
        car.roots(),
        car.roots().len()
    );
    println!("Total blocks: {}", car.len());
    println!();

    let mst: Mst = car
        .try_into()
        .map_err(|e| format!("Failed to convert CAR to Mst: {}", e))?;

    // traverse the mst and print all records in order (depth-first)

    println!("root mst: {:?}", &mst.root);

    println!("MST nodes count: {}", mst.nodes.len());

    // DFS traversal!
    for (i, (cid, node)) in mst.iter().enumerate() {
        println!("{}. CID: {}, Node: {:?}", i, cid, node);
    }

    Ok(())
}
```
