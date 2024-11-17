"""Create initial tables

Revision ID: 001
Revises: 
Create Date: 2024-01-17

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '001'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    # Create sales_records table
    op.create_table(
        'sales_records',
        sa.Column('id', sa.Integer(), nullable=False, autoincrement=True),
        sa.Column('date', sa.Date(), nullable=False),
        sa.Column('product_id', sa.Integer(), nullable=False),
        sa.Column('quantity', sa.Integer(), nullable=False),
        sa.Column('unit_price', sa.Numeric(10, 2), nullable=False),
        sa.Column('discount', sa.Numeric(5, 2), nullable=True, server_default='0.0'),
        sa.Column('total_sales', sa.Numeric(10, 2), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )

    # Create index on date column for faster queries
    op.create_index('idx_sales_records_date', 'sales_records', ['date'])

def downgrade():
    # Drop the sales_records table
    op.drop_table('sales_records')
