pub mod rrq {
    pub mod api {
        pub mod v1 {
            include!("gen/rrq.api.v1.rs");
        }
    }
    pub mod events {
        pub mod v1 {
            include!("gen/rrq.events.v1.rs");
        }
    }
}
