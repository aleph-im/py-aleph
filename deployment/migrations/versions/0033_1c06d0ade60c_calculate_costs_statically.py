"""calculate_costs_statically

Revision ID: 1c06d0ade60c
Revises: a3ef27f0db81
Create Date: 2025-02-05 10:19:24.814606

"""

from decimal import Decimal
from typing import Dict
from alembic import op
import sqlalchemy as sa
import logging

from aleph.db.accessors.cost import make_costs_upsert_query
from aleph.db.accessors.messages import get_message_by_item_hash
from aleph.services.cost import _get_product_price_type, get_detailed_costs
from aleph.types.cost import ProductComputeUnit, ProductPrice, ProductPriceOptions, ProductPriceType, ProductPricing
from aleph.types.db_session import DbSession

logger = logging.getLogger("alembic")


# revision identifiers, used by Alembic.
revision = "1c06d0ade60c"
down_revision = "a3ef27f0db81"
branch_labels = None
depends_on = None


hardcoded_initial_price: Dict[ProductPriceType, ProductPricing] = {
    ProductPriceType.PROGRAM: ProductPricing(
        ProductPriceType.PROGRAM,
        ProductPrice(
            ProductPriceOptions("0.05", "0.000000977"),
            ProductPriceOptions("200", "0.011")
        ),
        ProductComputeUnit(1, 2048, 2048)
    ),
    ProductPriceType.PROGRAM_PERSISTENT: ProductPricing(
        ProductPriceType.PROGRAM_PERSISTENT,
        ProductPrice(
            ProductPriceOptions("0.05", "0.000000977"),
            ProductPriceOptions("1000",  "0.055")
        ),
        ProductComputeUnit(1, 20480, 2048)
    ),
    ProductPriceType.INSTANCE: ProductPricing(
        ProductPriceType.INSTANCE,
        ProductPrice(
            ProductPriceOptions("0.05", "0.000000977"),
            ProductPriceOptions("1000", "0.055")
        ),
        ProductComputeUnit(1, 20480, 2048)
    ),
    ProductPriceType.INSTANCE_CONFIDENTIAL: ProductPricing(
        ProductPriceType.INSTANCE_CONFIDENTIAL,
        ProductPrice(
            ProductPriceOptions("0.05", "0.000000977"),
            ProductPriceOptions("2000", "0.11")
        ),
        ProductComputeUnit(1, 20480, 2048)
    ),
    ProductPriceType.STORAGE: ProductPricing(
        ProductPriceType.STORAGE,
        ProductPrice(
            ProductPriceOptions("0.333333333"),
        )
    ),
}



def do_calculate_costs() -> None:
    session = DbSession(bind=op.get_bind())

    msg_item_hashes = (
        session.execute(
           """
           SELECT m.item_hash, ms.status
                FROM messages m
                INNER JOIN message_status ms on (m.item_hash = ms.item_hash)
                WHERE ms.status = 'processed' and (m.type = 'INSTANCE' or m.type = 'PROGRAM' or m.type = 'STORE')
           """
            )
        .scalars()
        .all()
    )

    logger.debug("INIT: CALCULATE COSTS FOR: %r", msg_item_hashes)

    for item_hash in msg_item_hashes:
        message = get_message_by_item_hash(session, item_hash)
        
        if message:
            content = message.parsed_content
            type = _get_product_price_type(content)
            pricing = hardcoded_initial_price[type]
            costs = get_detailed_costs(session, content, message.item_hash, pricing)

            if len(costs) > 0:
                insert_stmt = make_costs_upsert_query(costs)
                session.execute(insert_stmt)


    logger.debug("FINISH: CALCULATE COSTS (%d)", len(msg_item_hashes))
    session.close()


def upgrade() -> None:

    op.execute("drop view costs_view")
    op.execute("drop view vm_costs_view")
    op.execute("drop view vm_volumes_files_view")

    op.create_table(
        "account_costs",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("owner", sa.String(), nullable=False),
        sa.Column("item_hash", sa.String(), nullable=False),
        sa.Column("type", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("ref", sa.String(), nullable=True),
        sa.Column("payment_type", sa.String(), nullable=False),
        sa.Column("cost_hold", sa.DECIMAL(), nullable=False, default=Decimal(0)),
        sa.Column("cost_stream", sa.DECIMAL(), nullable=False, default=Decimal(0)),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["item_hash"], ["messages.item_hash"], ondelete="CASCADE"
        ),
        sa.UniqueConstraint("owner", "item_hash", "type", "name"),
    )

    do_calculate_costs()



def downgrade() -> None:
    op.drop_table("account_costs")

    op.execute(
        """
        create or replace view vm_volumes_files_view as
            SELECT volume.program_hash AS vm_hash,
               volume.ref,
               volume.use_latest,
               'code_volume'::text AS type,
               tags.file_hash      AS latest,
               originals.file_hash AS original,
               CASE
                   WHEN volume.use_latest THEN tags.file_hash
                   ELSE originals.file_hash
                   END             AS volume_to_use
        FROM program_code_volumes volume
                 LEFT JOIN file_tags tags ON volume.ref::text = tags.tag::text
                 JOIN file_pins originals ON volume.ref::text = originals.item_hash::text
        UNION
        SELECT volume.program_hash AS vm_hash,
               volume.ref,
               volume.use_latest,
               'data_volume'::text AS type,
               tags.file_hash      AS latest,
               originals.file_hash AS original,
               CASE
                   WHEN volume.use_latest THEN tags.file_hash
                   ELSE originals.file_hash
                   END             AS volume_to_use
        FROM program_data_volumes volume
                 LEFT JOIN file_tags tags ON volume.ref::text = tags.tag::text
                 JOIN file_pins originals ON volume.ref::text = originals.item_hash::text
        UNION
        SELECT volume.program_hash AS vm_hash,
               volume.ref,
               volume.use_latest,
               'runtime'::text     AS type,
               tags.file_hash      AS latest,
               originals.file_hash AS original,
               CASE
                   WHEN volume.use_latest THEN tags.file_hash
                   ELSE originals.file_hash
                   END             AS volume_to_use
        FROM program_runtimes volume
                 LEFT JOIN file_tags tags ON volume.ref::text = tags.tag::text
                 JOIN file_pins originals ON volume.ref::text = originals.item_hash::text
        UNION
        SELECT volume.vm_hash,
               volume.ref,
               volume.use_latest,
               'machine_volume'::text AS type,
               tags.file_hash         AS latest,
               originals.file_hash    AS original,
               CASE
                   WHEN volume.use_latest THEN tags.file_hash
                   ELSE originals.file_hash
                   END                AS volume_to_use
        FROM vm_machine_volumes volume
                 LEFT JOIN file_tags tags ON volume.ref::text = tags.tag::text
                 JOIN file_pins originals ON volume.ref::text = originals.item_hash::text
        """
    )

    op.execute(
        """
        CREATE OR REPLACE VIEW vm_costs_view AS
        SELECT vm_versions.vm_hash,
               vm_versions.owner,
               vms.resources_vcpus,
               vms.resources_memory,
               file_volumes_size.file_volumes_size,
               other_volumes_size.other_volumes_size,
               used_disk.required_disk_space,
               cu.compute_units_required,
               bcp.base_compute_unit_price,
               m.compute_unit_price_multiplier,
               cpm.compute_unit_price,
               free_disk.included_disk_space,
               additional_disk.additional_disk_space,
               adp.disk_price,
               tp.total_price,
               ms.ms_status
        FROM vm_versions
                 JOIN vms ON vm_versions.current_version::text = vms.item_hash::text
                 LEFT JOIN (SELECT volume.vm_hash,
                              sum(files.size) AS file_volumes_size
                       FROM vm_volumes_files_view volume
                                LEFT JOIN files ON volume.volume_to_use::text = files.hash::text
                       GROUP BY volume.vm_hash) file_volumes_size
                      ON vm_versions.current_version::text = file_volumes_size.vm_hash::text
                 LEFT JOIN (SELECT instance_rootfs.instance_hash,
                                   instance_rootfs.size_mib::bigint * 1024 * 1024 AS rootfs_size
                            FROM instance_rootfs) rootfs_size ON vm_versions.vm_hash::text = rootfs_size.instance_hash::text
                 LEFT JOIN (SELECT vm_machine_volumes.vm_hash,
                              sum(vm_machine_volumes.size_mib) * 1024 * 1024 AS other_volumes_size
                       FROM vm_machine_volumes
                       GROUP BY vm_machine_volumes.vm_hash) other_volumes_size
                      ON vm_versions.current_version::text = other_volumes_size.vm_hash::text
                 LEFT JOIN (select message_status.item_hash, message_status.status as ms_status from message_status) ms ON vm_versions.vm_hash = ms.item_hash,
             LATERAL ( SELECT file_volumes_size.file_volumes_size +
                              other_volumes_size.other_volumes_size::numeric AS required_disk_space) used_disk,
             LATERAL ( SELECT ceil(GREATEST(ceil((vms.resources_vcpus / 1)::double precision),
                                            (vms.resources_memory / 2048)::double precision)) AS compute_units_required) cu,
             LATERAL ( SELECT CASE
                                  WHEN COALESCE(vms.persistent, true)
                                      THEN '21474836480'::bigint::double precision * cu.compute_units_required
                                  ELSE '2147483648'::bigint::double precision * cu.compute_units_required
                                  END AS included_disk_space) free_disk,
             LATERAL ( SELECT GREATEST((file_volumes_size.file_volumes_size + rootfs_size.rootfs_size::numeric +
                                        other_volumes_size.other_volumes_size::numeric)::double precision -
                                       free_disk.included_disk_space,
                                       0::double precision) AS additional_disk_space) additional_disk,
             LATERAL ( SELECT CASE
                                  WHEN vms.payment_type = 'superfluid' THEN 0
                                  WHEN COALESCE(persistent, true)
                                    AND environment_trusted_execution_policy IS NOT NULL 
                                    AND environment_trusted_execution_firmware IS NOT NULL THEN 2000
                                  WHEN COALESCE(persistent, true) THEN 1000
                                  ELSE 200
                                  END AS base_compute_unit_price) bcp,
             LATERAL ( SELECT CASE
                                  WHEN COALESCE(vms.persistent, true) THEN 1 + vms.environment_internet::integer
                                  ELSE 1
                                  END AS compute_unit_price_multiplier) m,
             LATERAL ( SELECT cu.compute_units_required * m.compute_unit_price_multiplier::double precision *
                              bcp.base_compute_unit_price::double precision AS compute_unit_price) cpm,
             LATERAL ( SELECT CASE
                        WHEN vms.payment_type = 'superfluid' THEN 0
                        ELSE additional_disk.additional_disk_space / 20::double precision / (1024 * 1024)::double precision
                        END AS disk_price
             ) adp,
             LATERAL ( SELECT cpm.compute_unit_price + adp.disk_price AS total_price) tp
             where ms.ms_status = 'processed'
        """
    )

    op.execute(
        """
        create or replace view costs_view as
            SELECT COALESCE(vm_prices.owner, storage.owner) AS address,
                   vm_prices.total_vm_cost,
                   sc.total_storage_cost,
                   tc.total_cost
            FROM (SELECT vm_costs_view.owner,
                         sum(vm_costs_view.total_price) AS total_vm_cost
                  FROM vm_costs_view
                  GROUP BY vm_costs_view.owner) vm_prices
                     FULL JOIN (SELECT file_pins.owner,
                                       sum(f.size) AS storage_size
                                FROM file_pins
                                         JOIN files f ON file_pins.file_hash::text = f.hash::text
                                WHERE file_pins.owner IS NOT NULL
                                GROUP BY file_pins.owner) storage ON vm_prices.owner::text = storage.owner::text,
                 LATERAL ( SELECT storage.storage_size / (3 * 1024 * 1024) AS total_storage_cost) sc,
                 LATERAL ( SELECT COALESCE(vm_prices.total_vm_cost, 0::double precision) +
                                  COALESCE(sc.total_storage_cost, 0::numeric)::double precision AS total_cost) tc
        """
    )
