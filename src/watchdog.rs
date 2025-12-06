use std::pin::Pin;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct WatchDog {
    deadline: Mutex<Pin<Box<tokio::time::Sleep>>>,
}

impl WatchDog {
    pub fn new(timeout: Duration) -> Self {
        Self {
            deadline: Mutex::new(Box::pin(tokio::time::sleep(timeout))),
        }
    }

    pub async fn reset(&self, timeout: Duration) {
        let mut deadline = self.deadline.lock().await;
        deadline.as_mut().reset(tokio::time::Instant::now() + timeout);
    }

    pub async fn wait(&self) {
        let mut deadline = self.deadline.lock().await;
        deadline.as_mut().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    #[tokio::test(start_paused = true)]
    async fn test_watchdog_timeout() -> anyhow::Result<()> {
        let watchdog = WatchDog::new(Duration::from_millis(1000));

        let timeout_task = tokio::spawn(async move {
            watchdog.wait().await;
        });

        // 仮想時間を500ms進める
        tokio::time::advance(Duration::from_millis(500)).await;
        // スケジューラに制御を渡して期限切れタスクを実行させる
        tokio::task::yield_now().await;
        assert!(!timeout_task.is_finished());

        // さらに498ms進める（合計998ms）
        tokio::time::advance(Duration::from_millis(498)).await;
        tokio::task::yield_now().await;
        assert!(!timeout_task.is_finished());

        // さらに3ms進める（合計1001ms）でタイムアウト
        tokio::time::advance(Duration::from_millis(3)).await;
        tokio::task::yield_now().await;
        assert!(timeout_task.is_finished());

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_watchdog_reset_with_different_timeout() -> anyhow::Result<()> {
        let watchdog = Arc::new(WatchDog::new(Duration::from_millis(1000)));
        let watchdog_clone = Arc::clone(&watchdog);

        let (tx, mut rx) = mpsc::unbounded_channel();
        let timeout_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = rx.recv() => {
                        // 異なるタイムアウト値（500ms）でリセット
                        watchdog_clone.reset(Duration::from_millis(500)).await;
                    }
                    _ = watchdog_clone.wait() => {
                        break;
                    }
                }
            }
        });

        // 初期タイムアウト: 1000ms
        tokio::time::advance(Duration::from_millis(999)).await;
        tokio::task::yield_now().await;
        assert!(!timeout_task.is_finished());

        // 500msの新しいタイムアウトでリセット
        tx.send(()).unwrap();
        tokio::task::yield_now().await;

        // 498ms経過（まだタイムアウトしない）
        tokio::time::advance(Duration::from_millis(498)).await;
        tokio::task::yield_now().await;
        assert!(!timeout_task.is_finished());

        // さらに3ms経過（合計501ms）でタイムアウト
        tokio::time::advance(Duration::from_millis(3)).await;
        tokio::task::yield_now().await;
        assert!(timeout_task.is_finished());

        Ok(())
    }
}
