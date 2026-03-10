pub mod shared {
    tonic::include_proto!("shared");
}

pub mod auth {
    tonic::include_proto!("auth");
}

pub mod shredstream {
    tonic::include_proto!("shredstream");
}

pub mod filtered {
    tonic::include_proto!("filtered");
}
