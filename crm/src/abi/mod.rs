pub mod auth;

use crate::{
    pb::{
        RecallRequest, RecallResponse, RemindRequest, RemindResponse, WelcomeRequest,
        WelcomeResponse,
    },
    CrmService,
};
use chrono::{Duration, Utc};
use crm_metadata::{
    pb::{Content, MaterializeRequest},
    Tpl,
};
use crm_send::pb::SendRequest;
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};
use tracing::warn;
use user_stat::pb::QueryRequest;

#[derive(Debug)]
struct EmailNotify {
    interval: u32,
    interval_for: String,
    subject: String,
    content: String,
}

impl EmailNotify {
    fn new(interval: u32, interval_for: String, subject: String, content: String) -> Self {
        EmailNotify {
            interval,
            interval_for,
            subject,
            content,
        }
    }
}

impl CrmService {
    pub async fn welcome(&self, req: WelcomeRequest) -> Result<Response<WelcomeResponse>, Status> {
        let request_id = req.id;

        let contents = self.get_contents(&req.content_ids).await?;

        let content = Tpl(&contents).to_string();

        let email_notify = EmailNotify::new(
            req.interval,
            "created_at".to_string(),
            "Welcome".to_string(),
            content,
        );

        self.send_notification(email_notify).await?;

        Ok(Response::new(WelcomeResponse { id: request_id }))
    }

    pub async fn recall(&self, req: RecallRequest) -> Result<Response<RecallResponse>, Status> {
        let request_id = req.id;

        let contents = self.get_contents(&req.content_ids).await?;

        let content = Tpl(&contents).to_string();

        let email_notify = EmailNotify::new(
            req.last_visit_interval,
            "last_visited_at".to_string(),
            "Recall".to_string(),
            content,
        );

        self.send_notification(email_notify).await?;

        Ok(Response::new(RecallResponse { id: request_id }))
    }

    pub async fn remind(&self, req: RemindRequest) -> Result<Response<RemindResponse>, Status> {
        let request_id = req.id;

        //let contents = vec![Content::new()];

        let email_notify = EmailNotify::new(
            req.last_visit_interval,
            "last_visited_at".to_string(),
            "Remind".to_string(),
            "Miss u".to_string(),
        );

        self.send_notification(email_notify).await?;

        Ok(Response::new(RemindResponse { id: request_id }))
    }

    async fn get_contents(&self, content_ids: &[u32]) -> Result<Vec<Content>, Status> {
        let contents = self
            .metadata
            .clone()
            .materialize(MaterializeRequest::new_with_ids(content_ids))
            .await?
            .into_inner();

        let contents: Vec<Content> = contents
            .filter_map(|v| async move { v.ok() })
            .collect()
            .await;

        Ok(contents)
    }

    async fn send_notification(&self, email_notify: EmailNotify) -> Result<(), Status> {
        let d1 = Utc::now() - Duration::days(email_notify.interval as _);
        let d2 = d1 + Duration::days(1);
        let query = QueryRequest::new_with_dt(email_notify.interval_for.as_str(), d1, d2);
        let mut res_user_stats = self.user_stats.clone().query(query).await?.into_inner();

        let (tx, rx) = mpsc::channel(1024);

        let sender = self.config.server.sender_email.clone();
        let subject = email_notify.subject;
        let content = email_notify.content;
        tokio::spawn(async move {
            let content = Arc::new(content.as_str());
            while let Some(Ok(user)) = res_user_stats.next().await {
                let sender = sender.clone();
                let subject = subject.clone();
                let tx = tx.clone();

                let req = SendRequest::new(subject, sender, &[user.email], &content);
                if let Err(e) = tx.send(req).await {
                    warn!("Failed to send message: {:?}", e);
                }
            }
        });
        let reqs = ReceiverStream::new(rx);

        // NOTE: this is an alternative solution
        // let sender = self.config.server.sender_email.clone();
        // let reqs = res.filter_map(move |v| {
        //     let sender: String = sender.clone();
        //     let contents = contents.clone();
        //     async move {
        //         let v = v.ok()?;
        //         Some(gen_send_req("Welcome".to_string(), sender, v, &contents))
        //     }
        // });

        self.notification.clone().send(reqs).await?;

        Ok(())
    }
}
