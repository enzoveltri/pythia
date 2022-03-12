from fastapi import APIRouter, Form, HTTPException, Depends
from starlette import status

from src.pythia.Constants import ACCESS_TOKEN_EXPIRE_MINUTES
from src.rest.Authentication import authenticate_user, create_access_token, get_current_active_user
from datetime import timedelta

from src.pythia.User import User

authroute = APIRouter()


@authroute.post("/login")
async def login_for_access_token(username: str = Form(...), password: str = Form(...)):
    user = authenticate_user(username, password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token}


@authroute.get("/me", response_model=str)
async def read_users_me(current_user: User = Depends(get_current_active_user)):
    print(current_user.username)
    return current_user.username