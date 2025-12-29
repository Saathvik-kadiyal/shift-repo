"""
Pydantic schemas for user authentication and user management.

This module defines request and response models used for user
registration, validation, and authentication workflows.
"""

# pylint: disable=too-few-public-methods
from pydantic import BaseModel, EmailStr, field_validator,Field

# BASE USER MODEL
class UserBase(BaseModel):
    """
    Base schema for user data.

    Contains shared fields and validation logic used across
    user-related request and response models.
    """
    username: str
    email: EmailStr = Field(..., example="user@mouritech.com")

    @field_validator("username")
    @classmethod
    def validate_username(cls, v):
        """
        Validate username length.

        Args:
            value (str): Username value.

        Returns:
            str: Validated username.

        Raises:
            ValueError: If username length is outside allowed range.
        """
        if not 3 <= len(v) <= 100:
            raise ValueError("Username must be between 3 and 100 characters")
        return v

    @field_validator("email")
    @classmethod
    def validate_email_domain(cls, v):
        """
        Ensure email belongs to the allowed domain.

        Args:
            value (EmailStr): Email address.

        Returns:
            EmailStr: Validated email.

        Raises:
            ValueError: If email domain is not allowed.
        """
        allowed_domains = "mouritech.com"
        domain = v.split("@")[-1].lower()
        if domain != allowed_domains:
            raise ValueError("Email domain must be of mouritech")
        return v


# USER CREATION SCHEMA
class UserCreate(UserBase):
    """
    Schema for user creation requests.

    Extends UserBase with password validation.
    """
    password: str

    @field_validator("password")
    @classmethod
    def validate_password(cls, v):
        """
        Validate password length.

        Args:
            value (str): Password value.

        Returns:
            str: Validated password.

        Raises:
            ValueError: If password length is invalid.
        """
        if not 8 <= len(v) <= 72:
            raise ValueError("Password must be between 8 and 72 characters")
        return v


# RESPONSE SCHEMA (For returning user info)
class UserResponse(BaseModel):
    """
    Response schema for returning user information.
    """
    id: int
    username: str
    email: str

    class Config:
        """Pydantic configuration."""
        from_attributes = True
