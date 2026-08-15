//! `tokio::select!` 内の `mpsc::Sender::send` がキャンセルされたときの最小再現です。

use std::collections::VecDeque;
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot, Notify};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkItem {
    pub id: &'static str,
}

impl WorkItem {
    pub fn new(id: &'static str) -> Self {
        Self { id }
    }
}

#[derive(Debug, Default)]
pub struct Outbox {
    pending: VecDeque<WorkItem>,
}

impl Outbox {
    pub fn with_item(item: WorkItem) -> Self {
        let mut pending = VecDeque::new();
        pending.push_back(item);
        Self { pending }
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }

    pub fn next_id(&self) -> Option<&'static str> {
        self.pending.front().map(|item| item.id)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum FlushOutcome {
    Sent,
    Stopped,
    ReceiverClosed,
}

/// バグ状態の実装です。
///
/// `send` が完了する前に停止分岐が選ばれると、`item` は `send` Future とともに
/// ドロップされます。しかし、Outbox からはすでに取り除かれています。
pub async fn flush_one(
    outbox: &mut Outbox,
    sender: &mpsc::Sender<WorkItem>,
    mut stop: oneshot::Receiver<()>,
    send_started: Arc<Notify>,
) -> FlushOutcome {
    let item = outbox
        .pending
        .pop_front()
        .expect("この最小再現ではOutboxに1件以上の項目が必要です");
    let item_id = item.id;

    eprintln!(
        "[outbox] id={item_id} を先に取り出しました: pending={}",
        outbox.len()
    );

    tokio::select! {
        result = async {
            send_started.notify_one();
            sender.send(item).await
        } => match result {
            Ok(()) => {
                eprintln!("[flush] id={item_id} の送信に成功しました");
                FlushOutcome::Sent
            }
            Err(_) => {
                eprintln!("[flush] receiverが閉じていたため id={item_id} を送信できませんでした");
                FlushOutcome::ReceiverClosed
            }
        },
        _ = &mut stop => {
            eprintln!("[flush] 停止通知を受けました。待機中のsend Futureはキャンセルされます");
            FlushOutcome::Stopped
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::error::TryRecvError;

    #[tokio::test]
    async fn stop_during_a_full_channel_must_preserve_the_pending_item() {
        let (sender, mut receiver) = mpsc::channel(1);
        sender.send(WorkItem::new("already-buffered")).await.unwrap();

        let mut outbox = Outbox::with_item(WorkItem::new("critical-request"));
        let (stop_tx, stop_rx) = oneshot::channel();
        let send_started = Arc::new(Notify::new());
        let wait_until_send_is_pending = Arc::clone(&send_started);

        let stop_after_send_started = async move {
            wait_until_send_is_pending.notified().await;
            stop_tx.send(()).unwrap();
        };

        let (outcome, ()) = tokio::join!(
            flush_one(&mut outbox, &sender, stop_rx, send_started),
            stop_after_send_started
        );

        assert_eq!(outcome, FlushOutcome::Stopped);
        assert_eq!(
            outbox.len(),
            1,
            "停止を選んだ場合、未送信の項目をOutboxから失ってはいけません"
        );
        assert_eq!(outbox.next_id(), Some("critical-request"));

        assert_eq!(receiver.recv().await, Some(WorkItem::new("already-buffered")));
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    }

    #[tokio::test]
    async fn available_capacity_sends_and_removes_the_pending_item() {
        let (sender, mut receiver) = mpsc::channel(1);
        let mut outbox = Outbox::with_item(WorkItem::new("normal-request"));
        let (_stop_tx, stop_rx) = oneshot::channel();
        let send_started = Arc::new(Notify::new());

        let outcome = flush_one(&mut outbox, &sender, stop_rx, send_started).await;

        assert_eq!(outcome, FlushOutcome::Sent);
        assert_eq!(outbox.len(), 0);
        assert_eq!(receiver.recv().await, Some(WorkItem::new("normal-request")));
    }
}
