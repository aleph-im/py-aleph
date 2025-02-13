"""fix VM cost view

Revision ID: 7bcb8e5fe186
Revises: f9fa39b6bdef
Create Date: 2023-08-04 15:14:39.082370

"""

from alembic import op


# revision identifiers, used by Alembic.
revision = "7bcb8e5fe186"
down_revision = "f9fa39b6bdef"
branch_labels = None
depends_on = None


def upgrade() -> None:
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
    op.execute(
        """
        create or replace view vm_costs_view as
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
               tp.total_price
            FROM vm_versions
                 JOIN vms ON vm_versions.current_version::text = vms.item_hash::text
                 JOIN (SELECT volume.vm_hash,
                              sum(files.size) AS file_volumes_size
                       FROM vm_volumes_files_view volume
                                LEFT JOIN files ON volume.volume_to_use::text = files.hash::text
                       GROUP BY volume.vm_hash) file_volumes_size
                      ON vm_versions.current_version::text = file_volumes_size.vm_hash::text
                 LEFT JOIN (SELECT instance_rootfs.instance_hash,
                              instance_rootfs.size_mib::bigint * 1024 * 1024 AS rootfs_size
                       FROM instance_rootfs) rootfs_size ON vm_versions.vm_hash::text = rootfs_size.instance_hash::text
                 JOIN (SELECT vm_machine_volumes.vm_hash,
                              sum(vm_machine_volumes.size_mib) * 1024 * 1024 AS other_volumes_size
                       FROM vm_machine_volumes
                       GROUP BY vm_machine_volumes.vm_hash) other_volumes_size
                      ON vm_versions.current_version::text = other_volumes_size.vm_hash::text,
             LATERAL ( SELECT file_volumes_size.file_volumes_size +
                              other_volumes_size.other_volumes_size::numeric AS required_disk_space) used_disk,
             LATERAL ( SELECT ceil(GREATEST(ceil((vms.resources_vcpus / 1)::double precision),
                                            (vms.resources_memory / 2048)::double precision)) AS compute_units_required) cu,
             LATERAL ( SELECT CASE
                                  WHEN COALESCE(vms.persistent, true)
                                      THEN '21474836480'::bigint::double precision * cu.compute_units_required
                                  ELSE 2147483648::double precision * cu.compute_units_required
                                  END AS included_disk_space) free_disk,
             LATERAL ( SELECT GREATEST((file_volumes_size.file_volumes_size + rootfs_size.rootfs_size::numeric +
                                        other_volumes_size.other_volumes_size::numeric)::double precision -
                                       free_disk.included_disk_space,
                                       0::double precision) AS additional_disk_space) additional_disk,
             LATERAL ( SELECT CASE
                                  WHEN COALESCE(vms.persistent, true) THEN 2000
                                  ELSE 200
                                  END AS base_compute_unit_price) bcp,
             LATERAL ( SELECT 1 + vms.environment_internet::integer AS compute_unit_price_multiplier) m,
             LATERAL ( SELECT cu.compute_units_required * m.compute_unit_price_multiplier::double precision *
                          bcp.base_compute_unit_price::double precision AS compute_unit_price) cpm,
         LATERAL ( SELECT additional_disk.additional_disk_space * 20::double precision /
                          (1024 * 1024)::double precision AS disk_price) adp,
         LATERAL ( SELECT cpm.compute_unit_price + adp.disk_price AS total_price) tp
         
        
        """
    )
    op.execute(
        """
        INSERT INTO error_codes(code, description) VALUES 
            (5, 'Insufficient balance')
        """
    )


def downgrade() -> None:
    op.execute("drop view costs_view")
    op.execute("drop view vm_costs_view")
    op.execute("drop view vm_volumes_files_view")
