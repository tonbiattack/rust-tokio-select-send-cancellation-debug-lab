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

/// 停止と送信待機を競合させながら、未送信項目の所有権を失わないようにします。
///
/// `Sender::reserve` は容量だけを先に確保するため、`select!` が停止分岐を選んで
/// 予約待機をキャンセルしても、`WorkItem` はまだOutboxに残っています。予約成功後は
/// `Permit::send` が同期的に項目を受け取るため、項目を取り出してから停止分岐と競合しません。
pub async fn flush_one(
    outbox: &mut Outbox,
    sender: &mpsc::Sender<WorkItem>,
    mut stop: oneshot::Receiver<()>,
    send_started: Arc<Notify>,
) -> FlushOutcome {
    let permit = tokio::select! {
        result = async {
            send_started.notify_one();
            sender.reserve().await
        } => match result {
            Ok(permit) => permit,
            Err(_) => {
                eprintln!("[flush] receiverが閉じていたため、Outboxの項目を送信しません");
                return FlushOutcome::ReceiverClosed;
            }
        },
        _ = &mut stop => {
            eprintln!("[flush] 停止通知を受けました。容量予約はキャンセルされ、項目はOutboxに残ります");
            return FlushOutcome::Stopped;
        }
    };

    let item = outbox
        .pending
        .pop_front()
        .expect("この最小再現ではOutboxに1件以上の項目が必要です");
    let item_id = item.id;
    eprintln!(
        "[outbox] id={item_id} をPermit取得後に取り出しました: pending={}",
        outbox.len()
    );

    permit.send(item);
    eprintln!("[flush] id={item_id} の送信に成功しました");
    FlushOutcome::Sent
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::error::TryRecvError;

    #[tokio::test]
    async fn stop_during_a_full_channel_must_preserve_the_pending_item() {
        let (sender, mut receiver) = mpsc::channel(1);
        sender
            .send(WorkItem::new("already-buffered"))
            .await
            .unwrap();

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

        assert_eq!(
            receiver.recv().await,
            Some(WorkItem::new("already-buffered"))
        );
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

    #[tokio::test]
    async fn a_closed_receiver_must_not_remove_the_pending_item() {
        let (sender, receiver) = mpsc::channel(1);
        drop(receiver);

        let mut outbox = Outbox::with_item(WorkItem::new("retry-later"));
        let (_stop_tx, stop_rx) = oneshot::channel();
        let send_started = Arc::new(Notify::new());

        let outcome = flush_one(&mut outbox, &sender, stop_rx, send_started).await;

        assert_eq!(outcome, FlushOutcome::ReceiverClosed);
        assert_eq!(outbox.len(), 1);
        assert_eq!(outbox.next_id(), Some("retry-later"));
    }
}
