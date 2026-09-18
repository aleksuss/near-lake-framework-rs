#![doc = include_str!("../README.md")]
#[macro_use]
extern crate derive_builder;

use futures::{Future, StreamExt};
use std::fmt::Debug;

pub use near_lake_context_derive::LakeContext;
pub use near_lake_primitives::{
    self,
    near_indexer_primitives::{self, near_primitives},
};

use crate::streamer::LakeMessage;
pub use aws_credential_types::Credentials;
use near_lake_primitives::{StreamerMessage, block::Block};
pub use types::{Lake, LakeBuilder, LakeContextExt, LakeError};

mod s3_fetchers;
mod streamer;
pub(crate) mod types;

pub(crate) const LAKE_FRAMEWORK: &str = "near_lake_framework";

struct EmptyContext;

impl<M> LakeContextExt<M> for EmptyContext {
    fn execute_before_run(&self, _message: &mut M) {}

    fn execute_after_run(&self) {}
}

#[allow(clippy::future_not_send)]
impl Lake {
    /// Creates `mpsc::channel` and returns the `receiver` to read the stream of `StreamerMessage`
    ///```no_run
    ///  # use near_lake_framework::LakeContext;
    ///
    /// #[derive(LakeContext)]
    ///  struct MyContext {
    ///      my_field: String,
    ///  }
    ///
    ///# fn main() -> anyhow::Result<()> {
    ///
    ///    let context = MyContext {
    ///       my_field: "my_value".to_string(),
    ///    };
    ///
    ///    near_lake_framework::LakeBuilder::default()
    ///        .testnet()
    ///        .start_block_height(112205773)
    ///        .build()?
    ///        .run_with_context(handle_block, &context)?;
    ///    Ok(())
    ///# }
    ///
    /// # async fn handle_block(_block: near_lake_primitives::block::Block, context: &MyContext) -> anyhow::Result<()> { Ok(()) }
    ///```
    pub fn run_with_context<'a, F, C, E, Fut>(
        self,
        f: F,
        context: &'a C,
    ) -> Result<(), Box<LakeError>>
    where
        F: Fn(Block, &'a C) -> Fut + 'a,
        C: LakeContextExt<Block>,
        Fut: Future<Output = Result<(), E>>,
        E: Into<Box<dyn std::error::Error>>,
    {
        self.run_message_with_context(f, context)
    }

    pub async fn run_with_context_async<'a, F, C, E, Fut>(
        self,
        f: F,
        context: &'a C,
    ) -> Result<(), Box<LakeError>>
    where
        F: Fn(Block, &'a C) -> Fut + 'a,
        C: LakeContextExt<Block>,
        Fut: Future<Output = Result<(), E>>,
        E: Into<Box<dyn std::error::Error>>,
    {
        self.run_message_with_context_async(f, context).await
    }

    /// Creates `mpsc::channel` and returns the `receiver` to read the stream of `StreamerMessage`
    ///```no_run
    ///# fn main() -> anyhow::Result<()> {
    ///    near_lake_framework::LakeBuilder::default()
    ///        .testnet()
    ///        .start_block_height(112205773)
    ///        .build()?
    ///        .run(|block| async move {
    ///            println!("Processing block {}", block.block_height());
    ///            Ok::<(), anyhow::Error>(())
    ///        })?;
    ///    Ok(())
    ///# }
    ///```
    pub fn run<F, Fut, E>(self, f: F) -> Result<(), Box<LakeError>>
    where
        F: Fn(Block) -> Fut,
        Fut: Future<Output = Result<(), E>>,
        E: Into<Box<dyn std::error::Error>>,
    {
        self.run_message(f)
    }

    /// Creates `mpsc::channel` and returns the `receiver` to read the stream of `StreamerMessage`
    ///```no_run
    ///#[tokio::main]
    ///# async fn main() -> anyhow::Result<()> {
    ///    near_lake_framework::LakeBuilder::default()
    ///        .testnet()
    ///        .start_block_height(112205773)
    ///        .build()?
    ///        .run(handle_block)?;
    ///    Ok(())
    ///# }
    ///
    /// # async fn handle_block(_block: near_lake_primitives::block::Block) -> anyhow::Result<()> { Ok(()) }
    ///```
    pub async fn run_async<F, Fut, E>(self, f: F) -> Result<(), Box<LakeError>>
    where
        F: Fn(Block) -> Fut,
        Fut: Future<Output = Result<(), E>>,
        E: Into<Box<dyn std::error::Error>>,
    {
        self.run_message_async(f).await
    }

    /// Runs a handler for each raw JSON streamer message.
    pub fn run_raw<F, Fut, E>(self, f: F) -> Result<(), Box<LakeError>>
    where
        F: Fn(Vec<u8>) -> Fut,
        Fut: Future<Output = Result<(), E>>,
        E: Into<Box<dyn std::error::Error>>,
    {
        self.run_message(f)
    }

    /// Asynchronously runs a handler for each raw JSON streamer message.
    pub async fn run_raw_async<F, Fut, E>(self, f: F) -> Result<(), Box<LakeError>>
    where
        F: Fn(Vec<u8>) -> Fut,
        Fut: Future<Output = Result<(), E>>,
        E: Into<Box<dyn std::error::Error>>,
    {
        self.run_message_async(f).await
    }

    /// Runs a handler for each low-level streamer message.
    pub fn run_streamer_messages<F, Fut, E>(self, f: F) -> Result<(), Box<LakeError>>
    where
        F: Fn(StreamerMessage) -> Fut,
        Fut: Future<Output = Result<(), E>>,
        E: Into<Box<dyn std::error::Error>>,
    {
        self.run_message(f)
    }

    /// Asynchronously runs a handler for each low-level streamer message.
    pub async fn run_streamer_messages_async<F, Fut, E>(self, f: F) -> Result<(), Box<LakeError>>
    where
        F: Fn(StreamerMessage) -> Fut,
        Fut: Future<Output = Result<(), E>>,
        E: Into<Box<dyn std::error::Error>>,
    {
        self.run_message_async(f).await
    }

    /// Runs a raw JSON handler with a context.
    pub fn run_raw_with_context<'a, F, C, E, Fut>(
        self,
        f: F,
        context: &'a C,
    ) -> Result<(), Box<LakeError>>
    where
        F: Fn(Vec<u8>, &'a C) -> Fut + 'a,
        C: LakeContextExt<Vec<u8>>,
        Fut: Future<Output = Result<(), E>>,
        E: Into<Box<dyn std::error::Error>>,
    {
        self.run_message_with_context(f, context)
    }

    /// Asynchronously runs a raw JSON handler with a context.
    pub async fn run_raw_with_context_async<'a, F, C, E, Fut>(
        self,
        f: F,
        context: &'a C,
    ) -> Result<(), Box<LakeError>>
    where
        F: Fn(Vec<u8>, &'a C) -> Fut + 'a,
        C: LakeContextExt<Vec<u8>>,
        Fut: Future<Output = Result<(), E>>,
        E: Into<Box<dyn std::error::Error>>,
    {
        self.run_message_with_context_async(f, context).await
    }

    /// Runs a low-level streamer-message handler with a context.
    pub fn run_streamer_messages_with_context<'a, F, C, E, Fut>(
        self,
        f: F,
        context: &'a C,
    ) -> Result<(), Box<LakeError>>
    where
        F: Fn(StreamerMessage, &'a C) -> Fut + 'a,
        C: LakeContextExt<StreamerMessage>,
        Fut: Future<Output = Result<(), E>>,
        E: Into<Box<dyn std::error::Error>>,
    {
        self.run_message_with_context(f, context)
    }

    /// Asynchronously runs a low-level streamer-message handler with a context.
    pub async fn run_streamer_messages_with_context_async<'a, F, C, E, Fut>(
        self,
        f: F,
        context: &'a C,
    ) -> Result<(), Box<LakeError>>
    where
        F: Fn(StreamerMessage, &'a C) -> Fut + 'a,
        C: LakeContextExt<StreamerMessage>,
        Fut: Future<Output = Result<(), E>>,
        E: Into<Box<dyn std::error::Error>>,
    {
        self.run_message_with_context_async(f, context).await
    }

    fn run_message<M, F, Fut, E>(self, f: F) -> Result<(), Box<LakeError>>
    where
        M: LakeMessage + Debug,
        F: Fn(M) -> Fut,
        Fut: Future<Output = Result<(), E>>,
        E: Into<Box<dyn std::error::Error>>,
    {
        let context = EmptyContext;
        self.run_message_with_context(move |message, _context| f(message), &context)
    }

    async fn run_message_async<M, F, Fut, E>(self, f: F) -> Result<(), Box<LakeError>>
    where
        F: Fn(M) -> Fut,
        M: LakeMessage + Debug,
        Fut: Future<Output = Result<(), E>>,
        E: Into<Box<dyn std::error::Error>>,
    {
        let context = EmptyContext;
        self.run_message_with_context_async(move |message, _context| f(message), &context)
            .await
    }

    fn run_message_with_context<'a, F, M, C, E, Fut>(
        self,
        f: F,
        context: &'a C,
    ) -> Result<(), Box<LakeError>>
    where
        F: Fn(M, &'a C) -> Fut + 'a,
        C: LakeContextExt<M>,
        M: LakeMessage + Debug,
        Fut: Future<Output = Result<(), E>>,
        E: Into<Box<dyn std::error::Error>>,
    {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|err| LakeError::RuntimeStartError { error: err })?;

        runtime.block_on(self.run_message_with_context_async(f, context))
    }

    async fn run_message_with_context_async<'a, M, F, C, E, Fut>(
        self,
        f: F,
        context: &'a C,
    ) -> Result<(), Box<LakeError>>
    where
        F: Fn(M, &'a C) -> Fut + 'a,
        C: LakeContextExt<M>,
        M: LakeMessage + Debug,
        Fut: Future<Output = Result<(), E>>,
        E: Into<Box<dyn std::error::Error>>,
    {
        let concurrency = self.concurrency;
        let (sender, stream) = streamer::streamer(self);

        let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
            .map(|mut streamer_message: M| async {
                context.execute_before_run(&mut streamer_message);

                let user_indexer_function_execution_result = f(streamer_message, context).await;

                context.execute_after_run();

                user_indexer_function_execution_result
            })
            .buffer_unordered(concurrency);

        while let Some(_handle_message) = handlers.next().await {}
        drop(handlers);

        match sender.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(Box::new(err)),
            Err(err) => Err(Box::new(err.into())),
        }
    }
}

#[cfg(test)]
mod handler_bounds_tests {
    use std::{cell::Cell, rc::Rc};

    use super::*;

    struct LocalContext(Rc<Cell<usize>>);

    impl LakeContextExt<Block> for LocalContext {
        fn execute_before_run(&self, _block: &mut Block) {
            self.0.set(self.0.get() + 1);
        }

        fn execute_after_run(&self) {}
    }

    // These functions are compile-time checks only: running them would start the S3 streamer.
    #[allow(dead_code)]
    fn blocking_run_accepts_local_state(lake: Lake, context: &LocalContext) {
        let local_state = Rc::new(Cell::new(0));

        let _ = lake.run_with_context(
            move |_block, _context| {
                let local_state = Rc::clone(&local_state);
                async move {
                    local_state.set(local_state.get() + 1);
                    Ok::<(), std::convert::Infallible>(())
                }
            },
            context,
        );
    }

    #[allow(dead_code)]
    async fn async_run_accepts_local_state(lake: Lake, context: &LocalContext) {
        let local_state = Rc::new(Cell::new(0));

        let _ = lake
            .run_with_context_async(
                move |_block, _context| {
                    let local_state = Rc::clone(&local_state);
                    async move {
                        local_state.set(local_state.get() + 1);
                        Ok::<(), std::convert::Infallible>(())
                    }
                },
                context,
            )
            .await;
    }
}
