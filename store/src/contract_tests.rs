// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Praxis Contributors

//! Reusable backend-contract suite, shared by the in-memory double and the SQL
//! backends. Enabled by the `test-support` feature. Each function drives one
//! backend through the trait surface and asserts the shared contract, so a new
//! backend proves conformance by running the same suite.

#![allow(clippy::expect_used, clippy::panic, reason = "reusable contract harness")]

use crate::{
    owner::StateOwner,
    traits::{ConversationItemStore, PersistedStateBackend},
    types::{ConversationItemRecord, ConversationRecord, PendingApprovalRecord, ResponseRecord},
};

/// Build a test owner from trusted parts.
fn owner(tenant: &str) -> StateOwner {
    StateOwner::from_trusted_parts(tenant, "issuer", "subject").unwrap_or_else(|err| panic!("owner: {err}"))
}

/// A conversation item scoped to `owner`/`conversation_id`.
fn item(owner: &StateOwner, conversation_id: &str, item_id: &str) -> ConversationItemRecord {
    ConversationItemRecord {
        item_id: item_id.to_owned(),
        owner: owner.clone(),
        conversation_id: conversation_id.to_owned(),
        item_data: serde_json::json!({ "id": item_id }),
        created_at: 0,
        position: 0,
    }
}

/// A pending approval fixture.
fn approval(id: &str) -> PendingApprovalRecord {
    PendingApprovalRecord {
        approval_id: id.to_owned(),
        server_label: "srv".to_owned(),
        tool_name: "tool".to_owned(),
        arguments: "{}".to_owned(),
        target_fingerprint: "fp".to_owned(),
    }
}

/// Run the whole backend contract suite. Panics on the first violation.
///
/// The backend must start empty. Callers run this inside their own async test.
///
/// # Panics
///
/// Panics if the backend violates the persistence contract.
pub async fn run_contract_suite(backend: &dyn PersistedStateBackend) {
    responses_are_owner_scoped(backend).await;
    approvals_consume_all_or_nothing(backend).await;
    conversation_messages_cas(backend).await;
    items_sync_positions_and_messages(backend).await;
}

/// A response is visible only to its owner, and delete is scoped.
async fn responses_are_owner_scoped(backend: &dyn PersistedStateBackend) {
    let (a, b) = (owner("resp-a"), owner("resp-b"));
    let record = ResponseRecord {
        id: "resp_1".to_owned(),
        owner: a.clone(),
        created_at: 1,
        model: "m".to_owned(),
        response_object: serde_json::json!({}),
        input: serde_json::json!({}),
        messages: serde_json::json!([]),
    };
    backend.upsert_response(&record).await.expect("upsert");
    assert!(
        backend.get_response(&a, "resp_1").await.expect("get a").is_some(),
        "owner sees own response"
    );
    assert!(
        backend.get_response(&b, "resp_1").await.expect("get b").is_none(),
        "response leaked across owners"
    );
    assert!(
        !backend.delete_response(&b, "resp_1").await.expect("delete b"),
        "cross-owner delete succeeded"
    );
    assert!(
        backend.delete_response(&a, "resp_1").await.expect("delete a"),
        "owner delete failed"
    );
}

/// `consume_approvals` is single-use and all-or-nothing.
#[expect(clippy::too_many_lines, reason = "linear contract assertions")]
async fn approvals_consume_all_or_nothing(backend: &dyn PersistedStateBackend) {
    let o = owner("appr");
    let approvals = [approval("a1"), approval("a2")];
    backend
        .record_pending_approvals(&o, "resp_appr", &approvals, 10)
        .await
        .expect("record");
    // Idempotent record: re-recording does not reset state.
    backend
        .record_pending_approvals(&o, "resp_appr", &approvals, 11)
        .await
        .expect("record twice");
    let found = backend
        .get_pending_approvals(&o, "resp_appr", &["a1", "a2"])
        .await
        .expect("get");
    assert_eq!(found.len(), 2, "both approvals retrievable");
    assert_eq!(
        backend
            .consume_approvals(&o, "resp_appr", &["a1"], 20)
            .await
            .expect("consume a1"),
        None,
        "first consume of an outstanding approval must succeed"
    );
    // A batch including the already-consumed a1 aborts wholesale.
    assert_eq!(
        backend
            .consume_approvals(&o, "resp_appr", &["a2", "a1"], 30)
            .await
            .expect("consume batch"),
        Some(1),
        "batch with a consumed id must abort at that index"
    );
    // a2 stayed outstanding through the aborted batch.
    assert_eq!(
        backend
            .consume_approvals(&o, "resp_appr", &["a2"], 40)
            .await
            .expect("consume a2"),
        None,
        "outstanding approval must survive an aborted batch"
    );
}

/// `compare_and_swap_conversation_messages` guards a stale write.
async fn conversation_messages_cas(backend: &dyn PersistedStateBackend) {
    let o = owner("cas");
    backend
        .upsert_conversation(&ConversationRecord {
            conversation_id: "conv_cas".to_owned(),
            owner: o.clone(),
            created_at: 1,
            metadata: serde_json::json!({}),
            messages: serde_json::json!(["v0"]),
        })
        .await
        .expect("upsert conversation");
    let v0 = serde_json::json!(["v0"]);
    let v1 = serde_json::json!(["v1"]);
    assert!(
        backend
            .compare_and_swap_conversation_messages(&o, "conv_cas", &v0, &v1)
            .await
            .expect("cas match"),
        "cas on the current value must succeed"
    );
    assert!(
        !backend
            .compare_and_swap_conversation_messages(&o, "conv_cas", &v0, &v1)
            .await
            .expect("cas stale"),
        "cas on a stale value must fail"
    );
}

/// `create_items_and_sync_messages` assigns sequential positions and rebuilds
/// the cache; `delete_item_and_sync_messages` rebuilds it again.
#[expect(clippy::too_many_lines, reason = "linear contract assertions")]
async fn items_sync_positions_and_messages(backend: &dyn PersistedStateBackend) {
    let o = owner("items");
    backend
        .upsert_conversation(&ConversationRecord {
            conversation_id: "conv_items".to_owned(),
            owner: o.clone(),
            created_at: 1,
            metadata: serde_json::json!({}),
            messages: serde_json::json!([]),
        })
        .await
        .expect("upsert conversation");
    backend
        .create_items_and_sync_messages(
            &o,
            "conv_items",
            &[item(&o, "conv_items", "it1"), item(&o, "conv_items", "it2")],
        )
        .await
        .expect("create items");
    assert_eq!(
        backend
            .conversation_item_position(&o, "conv_items", "it1")
            .await
            .expect("pos1"),
        Some(1),
        "first item assigned position 1"
    );
    assert_eq!(
        backend
            .conversation_item_position(&o, "conv_items", "it2")
            .await
            .expect("pos2"),
        Some(2),
        "second item assigned position 2"
    );
    assert_eq!(
        backend.max_item_position(&o, "conv_items").await.expect("max"),
        2,
        "max position is 2"
    );
    let listed = backend
        .list_conversation_items(&o, "conv_items", None, 10, true)
        .await
        .expect("list");
    assert_eq!(listed.len(), 2, "both items listed");
    assert!(
        backend
            .delete_item_and_sync_messages(&o, "conv_items", "it1")
            .await
            .expect("delete item"),
        "delete of an existing item must report true"
    );
    let remaining = ConversationItemStore::get_conversation(backend, &o, "conv_items")
        .await
        .expect("get conversation")
        .expect("conversation present");
    assert_eq!(
        remaining.messages.as_array().map(Vec::len),
        Some(1),
        "message cache rebuilt after delete"
    );
}

#[cfg(test)]
mod tests {
    use super::run_contract_suite;
    use crate::memory::InMemoryStore;

    #[tokio::test]
    async fn in_memory_backend_satisfies_the_contract() {
        run_contract_suite(&InMemoryStore::new()).await;
    }
}
