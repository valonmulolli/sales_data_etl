"""Initial setup

Revision ID: 001
Revises: 
Create Date: 2024-01-05

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.sql import func

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
        sa.Column('discount', sa.Numeric(5, 2), default=0.0),
        sa.Column('total_sales', sa.Numeric(10, 2), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=func.now()),
        
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create index on date and product_id for faster querying
    op.create_index('idx_sales_records_date', 'sales_records', ['date'])
    op.create_index('idx_sales_records_product_id', 'sales_records', ['product_id'])

def downgrade():
    # Drop indexes first
    op.drop_index('idx_sales_records_date', table_name='sales_records')
    op.drop_index('idx_sales_records_product_id', table_name='sales_records')
    
    # Drop sales_records table
    op.drop_table('sales_records')
