"""Exporta datos de grafo desde SQLite para visualizacion de red.

Uso:
    python graph_analysis.py

Salida:
    - graph_nodes.csv
    - graph_edges.csv
"""

import csv
import sqlite3

DB_FILE = 'drive_audit.sqlite3'
NODES_CSV = 'graph_nodes.csv'
EDGES_CSV = 'graph_edges.csv'


def export_nodes(conn):
    # Nodos tipo principal (usuarios, grupos, dominio, anyone)
    principal_nodes = conn.execute(
        """
        SELECT
            principal AS node_id,
            principal AS label,
            principal_type AS node_type,
            COUNT(DISTINCT file_id) AS degree
        FROM audit_edges
        GROUP BY principal, principal_type
        """
    ).fetchall()

    # Nodos tipo archivo
    file_nodes = conn.execute(
        """
        SELECT
            file_id AS node_id,
            COALESCE(name, file_id) AS label,
            'file' AS node_type,
            COUNT(*) AS degree
        FROM audit_files
        GROUP BY file_id, name
        """
    ).fetchall()

    with open(NODES_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'label', 'type', 'degree'])

        for node_id, label, node_type, degree in principal_nodes:
            writer.writerow([node_id, label, node_type, degree])

        for node_id, label, node_type, degree in file_nodes:
            writer.writerow([node_id, label, node_type, degree])


def export_edges(conn):
    # Aristas principal -> archivo
    rows = conn.execute(
        """
        SELECT
            e.principal AS source,
            e.file_id AS target,
            e.relation AS relation,
            COALESCE(e.role, '') AS role,
            COALESCE(e.allow_file_discovery, 0) AS allow_file_discovery,
            e.user_email AS audited_user
        FROM audit_edges e
        """
    ).fetchall()

    with open(EDGES_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'source',
            'target',
            'relation',
            'role',
            'allow_file_discovery',
            'audited_user',
        ])
        writer.writerows(rows)


def print_network_preview(conn):
    print('Preview de red (principal <-> principal por archivos compartidos):')
    pairs = conn.execute(
        """
        SELECT
            e1.principal AS principal_a,
            e2.principal AS principal_b,
            COUNT(DISTINCT e1.file_id) AS shared_files
        FROM audit_edges e1
        JOIN audit_edges e2
            ON e1.file_id = e2.file_id
           AND e1.principal < e2.principal
        GROUP BY e1.principal, e2.principal
        ORDER BY shared_files DESC
        LIMIT 20
        """
    ).fetchall()

    if not pairs:
        print('  (No hay conexiones para mostrar)')
        return

    for a, b, n in pairs:
        print(f'  - {a} <-> {b}: {n}')


def main():
    conn = sqlite3.connect(DB_FILE)
    try:
        export_nodes(conn)
        export_edges(conn)
        print_network_preview(conn)
        print(f'\nArchivos generados: {NODES_CSV}, {EDGES_CSV}')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
