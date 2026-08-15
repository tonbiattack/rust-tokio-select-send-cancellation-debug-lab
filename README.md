# rust-tokio-select-send-cancellation-debug-lab

Tokio の bounded MPSC チャネルに対し、`tokio::select!` 内で `Sender::send(item)` を待機すると、停止分岐が先に完了した場合に `item` が失われる問題を再現する最小プロジェクトです。

## 前提

| 項目 | 固定値 |
| --- | --- |
| Rust | 1.75.0 以上 |
| エディション | 2021 |
| Tokio | 1.37.0 |
| 外部サービス | 不要 |

依存バージョンは `Cargo.toml` と `Cargo.lock` で固定しています。再現では、容量 1 のチャネルを事前に満杯にし、`Notify` と `oneshot` で送信待機後にだけ停止通知を送ります。そのため、時刻や任意の `sleep` に依存しません。

## 何が起きるか

| 状態 | 実装 | 停止通知が送信待機に勝った場合 |
| --- | --- | --- |
| バグ状態 | Outbox から `item` を取り出し、`select!` 内で `send(item).await` | `item` は送信されず、Future とともにドロップされる |
| 修正状態 | `select!` 内で `reserve().await` し、`Permit` 取得後に Outbox から `item` を取り出す | `item` はまだ Outbox に残る |

Tokio 1.37.0 の `Sender::send` は、`select!` の別分岐が先に完了した場合にメッセージが送信されない一方、メッセージはドロップされ失われると説明しています。回避策として `reserve` と `Permit` を使うよう案内されています。[Tokio `Sender` documentation](https://docs.rs/tokio/1.37.0/tokio/sync/mpsc/struct.Sender.html)

## 修正状態の検証

```bash
cargo fmt --check
cargo test -- --nocapture
```

次の三つの契約テストがすべて成功します。

| テスト | 契約 |
| --- | --- |
| `stop_during_a_full_channel_must_preserve_the_pending_item` | 停止時に未送信の値を Outbox に残す |
| `available_capacity_sends_and_removes_the_pending_item` | 容量がある場合は送信して Outbox から除く |
| `a_closed_receiver_must_not_remove_the_pending_item` | 受信側クローズ時に Outbox の値を保持する |

## バグ状態の再現

作業中の変更がないことを確認してから、バグ状態のコミットへ切り替えます。

```bash
git switch --detach b9ed326
cargo test stop_during_a_full_channel_must_preserve_the_pending_item -- --nocapture
git switch master
```

バグ状態では、次の契約テストが失敗します。

```text
停止を選んだ場合、未送信の項目をOutboxから失ってはいけません
  left: 0
 right: 1
```

## Git履歴

| コミット | 内容 |
| --- | --- |
| `b9ed326` | 送信待機中のキャンセルで Outbox 項目を失う再現状態 |
| `887fcdb` | `reserve` 後に項目を取り出して `Permit::send` する最小修正 |

記事下書きは、コンテンツリポジトリの `private/07_AI生成下書き/06_テスト・デバッグ/デバッグ/` に配置しています。
