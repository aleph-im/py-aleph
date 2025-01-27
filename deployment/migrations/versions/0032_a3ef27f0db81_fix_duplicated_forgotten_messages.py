"""fix_duplicated_forgotten_messages

Revision ID: a3ef27f0db81
Revises: d8e9852e5775
Create Date: 2025-01-23 14:52:43.314424

"""

import asyncio
from threading import Thread

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from aleph.types.db_session import DbSession

from aleph.db.models.vms import VmBaseDb, VmVersionDb
import logging

logger = logging.getLogger("alembic")

# revision identifiers, used by Alembic.
revision = 'a3ef27f0db81'
down_revision = 'd8e9852e5775'
branch_labels = None
depends_on = None

def refresh_vm_version(session: DbSession, vm_hash: str) -> None:
    coalesced_ref = sa.func.coalesce(VmBaseDb.replaces, VmBaseDb.item_hash)
    select_latest_revision_stmt = (
        sa.select(
            coalesced_ref.label("replaces"),
            sa.func.max(VmBaseDb.created).label("created"),
        ).group_by(coalesced_ref)
    ).subquery()
    select_latest_program_version_stmt = (
        sa.select(
            coalesced_ref,
            VmBaseDb.owner,
            VmBaseDb.item_hash,
            VmBaseDb.created,
        )
        .join(
            select_latest_revision_stmt,
            (coalesced_ref == select_latest_revision_stmt.c.replaces)
            & (VmBaseDb.created == select_latest_revision_stmt.c.created),
        )
        .where(coalesced_ref == vm_hash)
    )

    insert_stmt = insert(VmVersionDb).from_select(
        ["vm_hash", "owner", "current_version", "last_updated"],
        select_latest_program_version_stmt,
    )
    upsert_stmt = insert_stmt.on_conflict_do_update(
        constraint="program_versions_pkey",
        set_={
            "current_version": insert_stmt.excluded.current_version,
            "last_updated": insert_stmt.excluded.last_updated,
        },
    )
    session.execute(sa.delete(VmVersionDb).where(VmVersionDb.vm_hash == vm_hash))
    session.execute(upsert_stmt)

def do_delete_vms(session: DbSession) -> None:
    # DELETE VMS

    vm_hashes = session.execute(
        """
        SELECT m.item_hash
            FROM messages m
            INNER JOIN forgotten_messages fm on (m.item_hash = fm.item_hash)
            WHERE m.type = 'INSTANCE' or m.type = 'PROGRAM'
        """
    ).scalars().all()

    logger.debug("DELETE VMS: %r", vm_hashes)

    session.execute(
        """
        DELETE
            FROM vms v
            WHERE v.item_hash in 
                (SELECT m.item_hash
                    FROM messages m
                    INNER JOIN forgotten_messages fm on (m.item_hash = fm.item_hash)
                    WHERE m.type = 'INSTANCE' or m.type = 'PROGRAM')
        """)
    
    session.execute(
        """
        DELETE
            FROM vms v
            WHERE v.replaces in 
                (SELECT m.item_hash
                    FROM messages m
                    INNER JOIN forgotten_messages fm on (m.item_hash = fm.item_hash)
                    WHERE m.type = 'INSTANCE' or m.type = 'PROGRAM')
        """)
    
    for item_hash in vm_hashes:
        refresh_vm_version(session, item_hash)

def do_delete_store(session: DbSession) -> None:
    # DELETE STORE

    session.execute(
        """
        DELETE 
        FROM file_pins fp 
	    WHERE fp.item_hash in (
		    SELECT m.item_hash 
		    FROM messages m
		    INNER JOIN forgotten_messages fm ON m.item_hash = fm.item_hash
		    WHERE m.type = 'STORE'
	    )
        """)

def do_delete_messages(session: DbSession) -> None:
    # DELETE MESSAGES

    session.execute(
        """
        DELETE
	        FROM messages m
	        using forgotten_messages fm 
	        WHERE m.item_hash = fm.item_hash
        """)

def do_delete(session: DbSession) -> None:
    """
        NOTE: We need to migrate (delete duplicates) from aggregate_elements, aggregates, file_tags and posts tables.
        The issue that was causing this inconsistent state has been fixed and we have considered that it doesn't worth to clean
        this tables for now as there are less than 1k orphan rows
    """
    do_delete_vms(session)
    do_delete_store(session)
    do_delete_messages(session)

async def upgrade_async() -> None:
    session = DbSession(bind=op.get_bind())
    do_delete(session)
    session.close()


def upgrade_thread():
    asyncio.run(upgrade_async())

def upgrade() -> None:
    thread = Thread(target=upgrade_thread, daemon=True)
    thread.start()
    thread.join()

def downgrade() -> None:
    pass
